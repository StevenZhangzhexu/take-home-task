# run_grid.py
import os
import itertools
import subprocess
from datetime import datetime

param_grid = {
    'spark.default.parallelism': [50, 100, 200],
    'spark.sql.shuffle.partitions': [50, 100],
    'spark.driver.memory': ['4g'],
    'spark.kryoserializer.buffer.max': ['512m']
}

def param_suffix(params):
    return '__'.join(f"{k.split('.')[-1]}={v}" for k, v in params.items())

def build_command(params, base_args):
    env_vars = ' \\ \\n'.join(
        f"    --conf {k}={v}" for k, v in params.items()
    )
    output_dir = os.path.join(
        base_args['output_dir'],
        param_suffix(params).replace('.', '_')
    )
    os.makedirs(output_dir, exist_ok=True)

    cmd = [
        'python', 'performance_test.py',
        f"--data_path={base_args['data_path']}",
        f"--config_path={base_args['config_path']}",
        f"--output_dir={output_dir}"
    ] + sum([["--conf", f"{k}={v}"] for k, v in params.items()], [])

    return cmd, output_dir

def run():
    base_args = {
        'data_path': 'data/large/',
        'config_path': 'config.yaml',
        'output_dir': 'results/grid_test'
    }

    keys, values = zip(*param_grid.items())
    all_combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]

    for params in all_combinations:
        print("\n== Running with config ==")
        for k, v in params.items():
            print(f"{k}: {v}")

        cmd, out_dir = build_command(params, base_args)
        log_path = os.path.join(out_dir, "run.log")
        with open(log_path, 'w') as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)

if __name__ == '__main__':
    run()
