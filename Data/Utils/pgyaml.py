import yaml


def yaml_load(*, yaml_filename):
    with open(yaml_filename, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def yaml_dump(*, data, yaml_filename):
    with open(yaml_filename, 'w') as outfile:
        yaml.dump(data, outfile, default_flow_style=False)
