"""Export the existing, leakage-safe 33-feature C-MAPSS split for a separate PyTorch candidate."""
import argparse
import json
import os
from pathlib import Path

import numpy as np
from pyspark.sql import functions as F

from pdm import spark_session
from pdm.windows import FEATURES
from train import group_dataset


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--fraction', type=float, default=.2)
    parser.add_argument('--fingerprint', required=True)
    args = parser.parse_args()
    if not 0 < args.fraction <= 1:
        raise ValueError('Invalid training fraction')
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    spark = spark_session('prepare-pytorch-rul')
    def collect(group, fraction=1.):
        return (group_dataset(spark, group, fraction).dropna(subset=FEATURES+['rul', 'failing'])
                .orderBy(F.xxhash64('machine_id', 'age')).limit(50000)
                .select(*FEATURES, 'rul', 'failing').toPandas())
    train, val = collect('train', args.fraction), collect('validation')
    x, vx = train[FEATURES].to_numpy(dtype=np.float32), val[FEATURES].to_numpy(dtype=np.float32)
    if len(x) < 2 or len(vx) < 2 or not np.isfinite(x).all() or not np.isfinite(vx).all():
        raise ValueError('C-MAPSS export is empty or contains invalid features')
    mean, std = x.mean(axis=0), x.std(axis=0)
    std[std < 1e-6] = 1
    temporary = root/'dataset.tmp.npz'
    np.savez_compressed(temporary, x=(x-mean)/std, y=train.rul.to_numpy(dtype=np.float32),
                        failing=train.failing.to_numpy(dtype=np.float32), vx=(vx-mean)/std,
                        vy=val.rul.to_numpy(dtype=np.float32), vfailing=val.failing.to_numpy(dtype=np.float32),
                        mean=mean, std=std)
    os.replace(temporary, root/'dataset.npz')
    metadata = {'dataset': 'cmapss-rul-33f-v1', 'fingerprint': args.fingerprint, 'features': FEATURES,
                'train_samples': len(x), 'validation_samples': len(vx), 'fraction': args.fraction,
                'split': 'existing train/validation/groups.json; replay excluded', 'window_cap_per_split': 50000}
    (root/'metadata.tmp').write_text(json.dumps(metadata), encoding='utf-8')
    os.replace(root/'metadata.tmp', root/'metadata.json')
    print(json.dumps(metadata), flush=True)
    spark.stop()


if __name__ == '__main__':
    main()
