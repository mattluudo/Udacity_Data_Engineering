def read_file(path, mode):
    with open(path, mode) as f:
        file = f.read()
        return file


class SqlQueries:
    path = '/home/md5822/airflow/plugins/sql/'
    eri_sql = read_file(path+'5th_ptile_qci_eri.sql', 'r')
    nok_sql = read_file(path+'5th_ptile_qci_nok.sql', 'r')
    test = read_file(path+'test.sql', 'r')



