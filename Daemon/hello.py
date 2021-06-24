import Communication.SMS.pgtwilio as pgtw
import os

if __name__ == '__main__':
    a = "  test.py arg1 arg2  "
    b = []
    for item in a.strip().split():
        b.append(item)
    A = pgtw.PGTwilio()

    print(b)
    try:
        print(os.environ['PYTHONPATH'])
        user_paths = os.environ['PYTHONPATH'].split(os.pathsep)
    except KeyError:
        user_paths = []
    print(user_paths)
    print("Worked")
