import os

from ruamel.yaml import YAML

def read_yaml(yaml_path):
    yaml = YAML(typ='safe', pure=True)
    with open(yaml_path) as fp:
        data = yaml.load(fp)
    fp.close()
    return data

def create_folder(full_path_name):
    if os.path.exists(full_path_name):
        if os.path.isdir(full_path_name):
            return
        elif os.path.isfile(full_path_name):
            raise FileExistsError(
                f"Non-directory file aleady exists at {full_path_name}"
            )
    else:
        os.makedirs(full_path_name)


def remove_file(full_path_name):
    if not os.path.isfile(full_path_name):
        raise FileNotFoundError(f"No file exists at {full_path_name}. Aborting")
    os.unlink(full_path_name)

