"""Gửi Bản tin vào Kafka. MemorySink cùng giao diện, dùng trong kiểm thử."""
import logging
import time

import orjson

log = logging.getLogger(__name__)

TOPICS = {"sensor.raw": 3, "machine.failed": 1}  # tên -> số partition


class KafkaSink:
    def __init__(self, bootstrap):
        from confluent_kafka import Producer

        self.bootstrap = bootstrap
        self.acked = self.errors = 0
        self.last_error_at = float("-inf")
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "error_cb": self._on_error})

    def ensure_topics(self):
        """Tạo hai topic nếu chưa có, không dựa vào tự tạo của broker (machine.failed chỉ có 1 partition)."""
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": self.bootstrap})  # giữ tham chiếu tới khi xong, nếu không lệnh bị hủy
        futures = admin.create_topics([NewTopic(name, partitions, 1) for name, partitions in TOPICS.items()])
        for name, f in futures.items():
            try:
                f.result(timeout=15)
                log.info("đã tạo topic %s", name)
            except Exception as e:  # đã có topic, hoặc Kafka chưa sẵn: trạng thái Kafka hiện trên giao diện
                log.info("topic %s: %s", name, e)

    def _on_error(self, err):
        self.errors += 1
        self.last_error_at = time.monotonic()
        log.warning("Kafka: %s", err)

    def _delivered(self, err, _msg):
        if err:
            self._on_error(err)
        else:
            self.acked += 1

    def send(self, topic, key, value):
        """False khi hàng đợi gửi đầy; bên gọi bỏ phần còn lại của nhịp thay vì để tiến trình chết."""
        try:
            self.producer.produce(topic, key=key, value=orjson.dumps(value), on_delivery=self._delivered)
        except BufferError:
            return False
        return True

    def poll(self):
        self.producer.poll(0)  # không chờ xác nhận: broker chậm không làm lệch nhịp

    def queue_len(self):
        return len(self.producer)

    @property
    def ok(self):
        return time.monotonic() - self.last_error_at > 5


class MemorySink:
    def __init__(self, capacity=None):
        self.messages = []  # (topic, key, value)
        self.capacity = capacity
        self.acked = self.errors = 0
        self.ok = True

    def send(self, topic, key, value):
        if self.capacity is not None and len(self.messages) >= self.capacity:
            return False
        self.messages.append((topic, key, value))
        self.acked += 1
        return True

    def poll(self):
        pass

    def queue_len(self):
        return 0
