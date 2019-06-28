import csv


if __name__ == '__main__':
    Path = '/home/milo/Develope/input/inegi/'
    File = 'SCIAN2018_Categorias-clase.csv'
    f = open(Path + File, 'r')
    scian_rama = f.readlines()
    f.close()

    data = [['id_act', 'actividad']]
    for line in scian_rama:
        x = line.split(',')
        if x[0].isdigit():
            aux = []
            for elem in x[:2]:
                aux.append(elem.replace('"', ''))
            data.append(aux)

    print('total activity rows : ', len(data))

    SaveFile = "../data_sample/denue_ramas.csv"
    with open(SaveFile, mode="w", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|',
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator='\n')
        for item in data:
            writer.writerow([item[0], item[1]])
