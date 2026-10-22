"""Cổng kiểm định: quy tắc quyết định ứng viên có thay Mô hình đang dùng hay không."""


def accepts(candidate, current):
    """Nhận khi RMSE hồi quy thấp hơn hẳn và F1 phân lớp không thấp hơn, đo trên cùng nhóm kiểm định."""
    return candidate["rul_rmse"] < current["rul_rmse"] and candidate["failing_f1"] >= current["failing_f1"]
