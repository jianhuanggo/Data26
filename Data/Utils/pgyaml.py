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


class folded_unicode(str): pass


class literal_unicode(str): pass


def folded_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='>')


def literal_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='|')


yaml.add_representer(folded_unicode, folded_unicode_representer)
yaml.add_representer(literal_unicode, literal_unicode_representer)
