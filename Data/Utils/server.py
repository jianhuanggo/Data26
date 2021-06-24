import subprocess


def ping(*, servername: str):
    output, result = subprocess.getstatusoutput(f"ping -c -w {servername}")
    if output == 0:
        return True
    else:
        return False

def telnet(*, servername: str, port: str):
    output, result = subprocess.getstatusoutput(f"telnet {servername} {port}")
    if output == 0:
        return True
    else:
        return False


if __name__ == '__main__':
    server_name = 'sanpaulo.cj9olslqxeit.sa-east-1.rds.amazonaws.com'
    ping(servername=server_name)
