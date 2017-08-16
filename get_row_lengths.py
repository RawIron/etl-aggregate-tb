lengths = []

with open("./getstat_com_serp_report_201707.csv") as f:
    line = f.readline()
    while line:
        lengths.append(len(line))
        line = f.readline()

print("Average %f" % (sum(lengths) / float(len(lengths)),))
print("Max %d Min %d" % (max(lengths), min(lengths)))

with open("./row_lengths.csv", 'w') as f:
    for number in lengths:
        f.write(str(number)+'\n')
