def read_file(path):
    with open(path, "r") as f:
        return f.read()


def append_file(path, content):
    with open(path, "a") as f:
        return f.write(content)


def write_file(path, content):
    with open(path, "w") as f:
        return f.write(content)



