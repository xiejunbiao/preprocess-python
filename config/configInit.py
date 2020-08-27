import configparser
import os
import sys


argv = sys.argv[1:]


def get_conf_dict(conf_file_path):
    # conf_file_path = os.path.join(conf_dir_path, 'config.ini')
    conf_obj = configparser.ConfigParser()
    conf_obj.read(conf_file_path)
    conf_dict = {section: dict(conf_obj.items(section)) for section in conf_obj.sections()}
    # port配置转化为Int类型方便连接mysql
    for k, v_dict in conf_dict.items():
        if 'port' in v_dict:
            v_dict['port'] = int(v_dict['port'])
    return conf_dict


config_dict = get_conf_dict(argv[1])
