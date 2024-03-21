file = 'PMs.txt'

n = 0
names = []
if __name__ == "__main__":
    with open(file, "r+") as f:
        lines = f.readlines()
        for line in lines:
            if line not in names:
                names.append(line)
    print('307 project numbers ')
    print('messages=pms: ', len(names))
