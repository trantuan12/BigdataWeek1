"""Export or restore the exact committed Delta objects referenced by the evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import read_json, write_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('export', 'verify', 'restore'))
    parser.add_argument('--config', default='config/team.json')
    parser.add_argument('--reuse-existing', action='store_true', help='Reuse a verified export for the unchanged table paths.')
    args = parser.parse_args()
    config = read_json(args.config)
    folder = ROOT / 'portable_delta'
    manifest_path = folder / 'snapshot_manifest.json'
    if args.action == 'export':
        state = read_json(Path(config['evidence_dir']) / 'state.json')
        bench = read_json(Path(config['evidence_dir']) / 'benchmark_report.json')
        paths = {layer: state[layer + '_path'] for layer in ('bronze', 'silver', 'gold')}
        paths.update({label: entry['path'] for label, entry in bench['layout_details'].items()})
        preflight_path = Path(config['evidence_dir']) / 'preflight.json'
        if preflight_path.is_file():
            paths['schema_probe'] = read_json(preflight_path)['delta_smoke_path']
        if manifest_path.exists():
            previous = read_json(manifest_path)
            if previous['table_uris'] == paths:
                if args.reuse_existing:
                    for entry in previous['objects']:
                        target = (folder / entry['file']).resolve()
                        if folder.resolve() not in target.parents:
                            raise ValueError('UNSAFE_SNAPSHOT_PATH')
                        data = target.read_bytes()
                        if len(data) != entry['bytes'] or hashlib.sha256(data).hexdigest() != entry['sha256']:
                            raise ValueError('SNAPSHOT_HASH_MISMATCH:' + entry['file'])
                    print(f"Reused verified export: {len(previous['objects'])} objects at the same recorded table URIs.")
                    return
                raise ValueError('SNAPSHOT_EXPORT_ALREADY_EXISTS: use verify for the frozen export')
            archive = (ROOT / config.get('archive_dir', '_archive_previous_snapshots')).resolve()
            archive.mkdir(parents=True, exist_ok=True)
            destination = archive / ('portable_delta_' + hashlib.sha256(manifest_path.read_bytes()).hexdigest()[:16])
            boundary = ROOT.parent.resolve() if config.get('archive_dir') else ROOT.resolve()
            if boundary not in destination.resolve().parents or ROOT.resolve() not in folder.resolve().parents:
                raise ValueError('ARCHIVE_OUTSIDE_WORKSPACE')
            folder.rename(destination)
        import boto3
        client = boto3.client('s3', endpoint_url=config['s3_endpoint'], region_name=config.get('s3_region', 'us-east-1'))
        records = []
        for table, uri in paths.items():
            parsed = urlparse(uri)
            prefix = parsed.path.lstrip('/').rstrip('/') + '/'
            for page in client.get_paginator('list_objects_v2').paginate(Bucket=parsed.netloc, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    relative = Path(parsed.netloc) / key
                    target = (folder / relative).resolve()
                    if folder.resolve() not in target.parents:
                        raise ValueError('UNSAFE_OBJECT_KEY')
                    data = client.get_object(Bucket=parsed.netloc, Key=key)['Body'].read()
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                    records.append({'table': table, 'bucket': parsed.netloc, 'key': key,
                                    'file': relative.as_posix(), 'bytes': len(data),
                                    'sha256': hashlib.sha256(data).hexdigest()})
        write_json(manifest_path, {'purpose': 'Exact Delta logs and data files; historical versions preserved.',
                   'table_uris': paths, 'objects': records})
        print(f'Exported {len(records)} Delta objects, including transaction logs.')
        return
    manifest = read_json(manifest_path)
    for entry in manifest['objects']:
        target = (folder / entry['file']).resolve()
        if folder.resolve() not in target.parents:
            raise ValueError('UNSAFE_SNAPSHOT_PATH')
        data = target.read_bytes()
        if len(data) != entry['bytes'] or hashlib.sha256(data).hexdigest() != entry['sha256']:
            raise ValueError('SNAPSHOT_HASH_MISMATCH:' + entry['file'])
    if args.action == 'restore':
        import boto3
        client = boto3.client('s3', endpoint_url=config['s3_endpoint'], region_name=config.get('s3_region', 'us-east-1'))
        # Refuse the entire operation before writes if any selected table prefix exists.
        for uri in manifest['table_uris'].values():
            parsed = urlparse(uri)
            existing = client.list_objects_v2(Bucket=parsed.netloc,
                Prefix=parsed.path.lstrip('/').rstrip('/') + '/', MaxKeys=1)
            if existing.get('KeyCount', 0):
                raise ValueError('RESTORE_REQUIRES_EMPTY_TABLE_PREFIX:' + uri)
        for entry in manifest['objects']:
            client.put_object(Bucket=entry['bucket'], Key=entry['key'], Body=(folder / entry['file']).read_bytes())
        print('Restored exact Delta objects to their original table URIs. Run live verification next.')
    else:
        print(f"PASS: {len(manifest['objects'])} portable Delta object hashes.")


if __name__ == '__main__':
    main()
