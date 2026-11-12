from pathlib import Path
import csv,hashlib,json,sqlite3,tarfile,tempfile
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'artifacts/deployment'
archive=OUT/'bee-ridge-atspm-data-2026-10-15.tar.gz'
raw=sorted((ROOT/'derived/bee_ridge_atspm_feb_to_may').glob('atspm-*_filtered.csv'))
ids=[r['ATMS_ID'] for r in csv.DictReader((ROOT/'atspm/BeeRidgeIntersections.csv').open())]
dbs=[ROOT/'derived/reference_data.sqlite']+[ROOT/f'derived/dask_historical_{sid}_all_days_feb_to_may/atspm_hourly_results.sqlite' for sid in ids]
assert raw and all(p.exists() for p in dbs)
manifest={}
with tempfile.TemporaryDirectory(prefix='bee-data-') as tmp:
    snapshots=[]
    for i,src in enumerate(dbs):
        dst=Path(tmp)/f'{i}.sqlite'
        with sqlite3.connect(src.as_uri()+'?mode=ro',uri=True) as source, sqlite3.connect(dst) as target:
            source.backup(target)
            assert target.execute('PRAGMA quick_check').fetchone()[0]=='ok'
        snapshots.append((dst,src.relative_to(ROOT)))
    entries=[(p,p.relative_to(ROOT)) for p in raw]+snapshots
    override=ROOT/'derived/ctrl01240_atspm/detector_config_1240.csv'
    if override.exists():entries.append((override,override.relative_to(ROOT)))
    print(f'Packaging {len(raw)} raw files, {len(dbs)} SQLite snapshots; {sum(p.stat().st_size for p,_ in entries)/1e9:.2f} GB uncompressed',flush=True)
    part=archive.with_suffix('.partial')
    with tarfile.open(part,'w:gz',compresslevel=1) as tar:
        for i,(p,rel) in enumerate(entries):
            h=hashlib.sha256()
            with p.open('rb') as f:
                for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
            manifest[str(rel)]={'bytes':p.stat().st_size,'sha256':h.hexdigest()}
            tar.add(p,arcname=str(rel),recursive=False)
            if i%15==0:print(f'Packed {i+1}/{len(entries)}',flush=True)
        m=Path(tmp)/'DATA_MANIFEST.json';m.write_text(json.dumps(manifest,indent=2)+'\n');tar.add(m,arcname='DATA_MANIFEST.json')
    part.replace(archive)
    h=hashlib.sha256()
    with archive.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    archive.with_name(archive.name+'.sha256').write_text(f'{h.hexdigest()}  {archive.name}\n')
    print(f'Created {archive}; {archive.stat().st_size/1e6:.1f} MB',flush=True)
