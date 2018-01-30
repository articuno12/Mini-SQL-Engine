import sqlparse
import sys
import copy

BADVAL = -99999999

def ToInt(s):
    try:
        s = int(s)
    except:
        s = int(s[1:-1])
    return s
def aggregate(table, col, func):
    if func.lower() == 'min':
        # print "here",min(table[col])
        table[col] = [min([num for num in table[col] if num != BADVAL])]
    elif func.lower() == 'max':
        table[col] = [max(table[col])]
    elif func.lower() == 'sum':
        s = 0
        for num in table[col]:
            if num!=BADVAL : s += num
        table[col] = [s]
    elif func.lower() == 'distinct':
        table[col] = list(set(table[col]))
    elif func.lower() == 'count' :
        l = 0
        for num in table[col] :
            if num != BADVAL : l += 1
        table[col] = [l]
    elif func.lower() == 'avg' :
        s=0
        l=0
        for num in table[col] :
            if num!=BADVAL :
                s += num
                l += 1

        table[col] = [float(s)/float(l)]
    return table

def readMetadata(dictionary):
    f = open('metadata.txt', 'r')
    tag = False
    for line in f:
        if line.strip() == "<begin_table>":
            tag = True
            continue
        if tag:
            tableName = line.strip()
            dictionary[tableName] = []
            tag = False
            continue
        if not line.strip() == '<end_table>':
            dictionary[tableName].append(line.strip())


def evaluate_csv(tabs):
    tabs = tabs + ".csv"
    m = []
    try:
        with open(tabs, "r+") as fp2:
            j = fp2.readlines()
    except:
        print ('This table does not exists')
        sys.exit(0)
    for k in j:
        m.append(k.rstrip("\r\n"))
    return m

def preprocess():
    meta_dict = {}
    readMetadata(meta_dict)

    # whole database as rows
    db_row_dict = {}
    for t in meta_dict:
        db_row_dict[t] = evaluate_csv(t)

    # convert in dictionary representation
    db_col_dict = {}
    for tab in meta_dict:
        db_col_dict[tab] = {}
        for col in meta_dict[tab]:
            db_col_dict[tab][col] = []

    for tab in db_row_dict:
        for row in db_row_dict[tab]:
            row = row.split(',')
            for i in range(len(row)):
                col_name = meta_dict[tab][i]
                db_col_dict[tab][col_name].append(ToInt(row[i]))

    return meta_dict, db_row_dict, db_col_dict

def apply_op(val1, val2, op):
    if op == '=':
        return val1 != val2
    elif op == '<':
        return val1 >= val2
    elif op == '>':
        return val1 <= val2
    elif op == '<=':
        return val1 > val2
    elif op == '>=':
        return val1 < val2
    else:
        print(str(op) + ' operator not recognized')
        sys.exit(0)

def print_beautify(table1, table2, table3, conjc, col_list):
    if conjc == 'and':
        for c in table2:
            for i in range(len(table2[c])):
                if table2[c][i] == BADVAL or table3[c][i] == BADVAL:
                    # print('not here')
                    table1[c][i] = BADVAL
    elif conjc == 'or':
        for c in table2:
            for i in range(len(table2[c])):
                if table2[c][i] == BADVAL and table3[c][i] == BADVAL:
                    table1[c][i] = BADVAL
    else:
        table1 = table2
    col_print = [c.strip() for c in col_list]
    check_dup = []
    if(col_list[0] == '*'):
        col_print = []
        for c in table1:
            raw_name = (c.split('.'))[-1]
            if raw_name not in check_dup:
                check_dup.append(raw_name)
                col_print.append(c)

    for c in col_print:
        print str(c)+"\t",
    print
    for c in col_print:
        if '(' in c:
            func = (c.split('('))[0]
            c = (((c.split('('))[1]).split(')'))[0]
            table1 = aggregate(table1, c, func)

            c = col_print[0]
            col_print = [(((c.split('('))[1]).split(')'))[0]]

    for i in range(len(table1[col_print[0]])):
        print_flag = False
        for j in range(len(col_print)):
            if table1[col_print[j]][i] != BADVAL:
                print_flag = True
                if(type(table1[col_print[j]][i]) == type(4.5)):
                    print str(format(table1[col_print[j]][i],'.2f')) + "\t",
                else:
                    print str(table1[col_print[j]][i]) + "\t",
        if print_flag:
            print
    # print

def main():
    meta_dict, db_row_dict, db_col_dict = preprocess()
    #print meta_dict, db_col_dict, db_row_dict
    command = sys.argv[1]
    query = sqlparse.parse(command)[0]

    if str(query.tokens[0]).lower() == "select":
        conjuction = None
        col_term = str(query.tokens[2])
        column_list = [x.strip() for x in col_term.split(',') if x]

        final_table = {}
        if str(query.tokens[4]).lower() == "from":
            table_term = (str(query.tokens[6])).strip()
            table_list = [x.strip() for x in table_term.split(',') if x]

            for t in table_list:
                if t not in meta_dict:
                    print('Table does not exist')
                    sys.exit(0)
            if len(table_list) > 1:
                for i in range(len(column_list)):
                    for tab in meta_dict:
                        for col in meta_dict[tab]:
                            if(column_list[i] == col):
                                column_list[i] = str(tab)+'.'+str(col)
                cross_table = {}
                flag = False
                for tab in table_list:
                    for col in meta_dict[tab]:
                        temp_col_name = str(tab)+'.'+str(col)
                        col_arr = db_col_dict[tab][col]
                        if flag:
                            col_arr = col_arr * len(col_arr)
                        else:
                            col_arr = [item for item in col_arr for i in range(len(col_arr))]
                        cross_table[temp_col_name] = col_arr
                        # print tab, cross_table, col_arr
                    flag = True
                final_table = cross_table
            else:
                final_table = db_col_dict[table_list[0]]
            dup_table = copy.deepcopy(final_table)
            fresh_table = copy.deepcopy(final_table)
            if len(query.tokens) >= 9:
                whereclause = query.tokens[8]
                if str(whereclause.tokens[0]).lower() == "where":
                    comparison = whereclause.tokens[2]
                    key = str(comparison.tokens[0])
                    op = str(comparison.tokens[2])
                    try:
                        value = ToInt(str(comparison.tokens[4]))
                        for i in range(len(final_table[key])):
                            if apply_op(final_table[key][i], value, op):
                                for col_head in final_table:
                                    final_table[col_head][i] = BADVAL
                    except:
                        value = str(comparison.tokens[4])
                        for i in range(len(final_table[key])):
                            if apply_op(final_table[key][i], final_table[value][i], op):
                                for col_head in final_table:
                                    final_table[col_head][i] = BADVAL
                    try:
                        conjuction = str(whereclause.tokens[4])
                        comparison = whereclause.tokens[6]
                        key = str(comparison.tokens[0])
                        op = str(comparison.tokens[2])
                        try:
                            value = ToInt(str(comparison.tokens[4]))
                            for i in range(len(dup_table[key])):
                                if apply_op(dup_table[key][i], value, op):
                                    for col_head in dup_table:
                                        dup_table[col_head][i] = BADVAL
                        except:
                            value = str(comparison.tokens[4])
                            for i in range(len(dup_table[key])):
                                if apply_op(dup_table[key][i], dup_table[value][i], op):
                                    for col_head in dup_table:
                                        dup_table[col_head][i] = BADVAL
                    except:
                        conjuction = None
            print_beautify(fresh_table, final_table, dup_table, conjuction, column_list)
        else:
            print("Invalid Syntax ")
            sys.exit(0)
    else:
        print("Unsupported Query")
        sys.exit(0)

if __name__ == "__main__":
    main()
