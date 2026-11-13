"""Real C-MAPSS RUL/failure candidate, separate from serving Spark PipelineModels."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

import numpy as np
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, DistributedSampler, TensorDataset


def emit(**value):
    payload = ('TRAINING_EVENT '+json.dumps(value, ensure_ascii=False, allow_nan=False)+'\n').encode()
    if len(payload) > 4096:
        raise ValueError('Telemetry frame exceeds atomic pipe size')
    os.write(1, payload)


def log(message):
    return {'at': datetime.now(timezone.utc).isoformat(), 'level': 'info', 'message': message}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--device', choices=['cpu', 'cuda'], required=True)
    parser.add_argument('--epochs', type=int, required=True)
    parser.add_argument('--batch-size', type=int, required=True)
    parser.add_argument('--learning-rate', type=float, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--data', default='/data/training.npz')
    parser.add_argument('--fingerprint', default='')
    args = parser.parse_args()
    torch.set_num_threads(1)
    local = int(os.environ['LOCAL_RANK'])
    if args.device == 'cuda':
        if not torch.cuda.is_available() or local >= torch.cuda.device_count():
            raise RuntimeError('Requested GPU does not exist')
        torch.cuda.set_device(local)
    device = torch.device(f'cuda:{local}' if args.device == 'cuda' else 'cpu')
    dist.init_process_group('nccl' if args.device == 'cuda' else 'gloo')
    rank, world = dist.get_rank(), dist.get_world_size()
    torch.manual_seed(42)
    data = np.load(args.data, allow_pickle=False)
    x = torch.from_numpy(data['x']).float()
    labels = torch.column_stack((torch.from_numpy(data['y']).float()/125, torch.from_numpy(data['failing']).float()))
    vx = torch.from_numpy(data['vx'][rank::world]).float().to(device)
    vy = torch.from_numpy(data['vy'][rank::world]).float().to(device)
    vf = torch.from_numpy(data['vfailing'][rank::world]).float().to(device)
    ds = TensorDataset(x, labels)
    sampler = DistributedSampler(ds, seed=42)
    loader = DataLoader(ds, batch_size=args.batch_size, sampler=sampler)
    model = DDP(torch.nn.Sequential(torch.nn.Linear(x.shape[1], 64), torch.nn.ReLU(), torch.nn.Linear(64, 32),
                                  torch.nn.ReLU(), torch.nn.Linear(32, 2)).to(device), device_ids=[local] if args.device == 'cuda' else None)
    optimizer = torch.optim.Adam(model.parameters(), lr=args.learning_rate)
    args.output.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    emit(ranks=[{'rank': rank, 'device': str(device), 'status': 'running', 'epoch': 0}],
         logs=[log(f'Rank {rank}/{world}: C-MAPSS thật, {len(x)} mẫu train, {len(data["vx"])} validation, {x.shape[1]} đặc trưng.')])
    try:
        for epoch in range(1, args.epochs+1):
            sampler.set_epoch(epoch)
            model.train()
            losses, count = 0., 0
            for batch, target in loader:
                batch, target = batch.to(device), target.to(device)
                optimizer.zero_grad(set_to_none=True)
                prediction = model(batch)
                loss = torch.nn.functional.mse_loss(prediction[:, 0], target[:, 0]) + .2*torch.nn.functional.binary_cross_entropy_with_logits(prediction[:, 1], target[:, 1])
                loss.backward()
                optimizer.step()
                losses += loss.item()*len(target)
                count += len(target)
            model.eval()
            with torch.no_grad():
                out = model.module(vx)
                rul = (out[:, 0]*125).clamp(0,125)
                flagged, actual = out[:, 1] >= 0, vf >= .5
                values = torch.tensor([losses, count, ((rul-vy)**2).sum().item(), len(vy),
                                       (flagged == actual).sum().item(), (flagged & actual).sum().item(),
                                       (flagged & ~actual).sum().item(), (~flagged & actual).sum().item()], dtype=torch.float64, device=device)
                dist.all_reduce(values)
            loss_value = float(values[0]/values[1])
            metric = {'epoch': epoch, 'loss': round(loss_value,6), 'accuracy': float(values[4]/values[3]),
                      'rul_rmse': float((values[2]/values[3]).sqrt()),
                      'failing_f1': float(2*values[5]/(2*values[5]+values[6]+values[7])) if values[5] else 0.,
                      'elapsed_seconds': round(time.monotonic()-started,3)}
            emit(ranks=[{'rank': rank, 'device': str(device), 'status': 'running', 'epoch': epoch, 'loss': loss_value}])
            if rank == 0:
                torch.save({'model': model.module.state_dict(), 'optimizer': optimizer.state_dict(), 'epoch': epoch,
                            'features': 33, 'normalization_mean': data['mean'], 'normalization_std': data['std'],
                            'dataset': 'cmapss-rul-33f-v1', 'fingerprint': args.fingerprint}, args.output/'checkpoint.tmp')
                os.replace(args.output/'checkpoint.tmp', args.output/'checkpoint.pt')
                emit(completed_epochs=epoch, metrics=[metric], logs=[log(f'Epoch {epoch}/{args.epochs}: RUL RMSE={metric["rul_rmse"]:.3f}, F1={metric["failing_f1"]:.3f}')])
            dist.barrier()
        emit(ranks=[{'rank': rank, 'device': str(device), 'status': 'succeeded', 'epoch': args.epochs, 'loss': loss_value}])
        if rank == 0:
            result = {'dataset': 'cmapss-rul-33f-v1', 'fingerprint': args.fingerprint, 'epochs': args.epochs,
                      'world_size': world, 'device': args.device, **metric,
                      'n_features': 33, 'train_samples': len(x), 'validation_samples': len(data['vx'])}
            (args.output/'result.json').write_text(json.dumps(result, allow_nan=False), encoding='utf-8')
            emit(artifact_path=str(args.output/'checkpoint.pt'), completed_epochs=args.epochs,
                 logs=[log('Đã lưu ứng viên PyTorch RUL; model Spark đang phục vụ được giữ nguyên.')])
    finally:
        dist.destroy_process_group()


if __name__ == '__main__':
    main()
