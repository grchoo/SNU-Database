from lark import Lark, Transformer
from berkeleydb import db
from pprint import pprint

#db파일을 연다
myDB=db.DB()
myDB.open('myDB.db', dbtype=db.DB_HASH)

def print_prompt():
    print('DB_2019-12946> ', end='')

#lark 파일의 변수 명과 아래 class의 함수명이 동일하게 정의
class MyTransformer(Transformer):
    def create_table_query(self, items):
        print_prompt()
        tnameString=str(items[2].pretty()).split('\t')[1].strip().lower()
        #파싱된 쿼리에서 테이블 이름을 가져온다
        tname=bytes(tnameString, encoding='utf-8')
        #파싱된 쿼리에서 컬럼, primary key, foreign key 등의 정보를 가져온다
        tel=str(items[3].pretty())

        #같은 이름의 테이블이 이미 존재하는 지 검사한다
        cursor=myDB.cursor()
        if(myDB.get(tname)==tname):
            print('Create table has failed: table with the same name already exists')
            return 'TableExistenceError'

        columnSet=[]
        columnList=[]
        typeList=[]
        notnullList=[]
        primaryList=[]
        foreignList=[]

        #컬럼 이름, 타입, null 여부 정보를 가져온다.
        for x in items[3].find_data('table_element_list'):
            for y in x.find_data('table_element'):
                for z in y.find_data('column_definition'):
                    column=""
                    #컬럼 이름을 가져온다
                    for w in z.find_data('column_name'):
                        column=str(w.pretty()).split('\t')[1].strip().lower()
                        if column in columnList:
                            print('Create table has failed: column definition is duplicated')
                            return 'DuplicateColumnDefError'
                        else:
                            columnList.append(column)
                    #데이터 타입을 가져온다
                    for k in z.find_data('data_type'):
                        temp = ""
                        for v in str(k.pretty()).split('\n'):
                            temp = temp + v.strip()
                        if temp.find('char') != -1:
                            if int(temp[temp.find('(') + 1:temp.find(')')]) < 1:
                                print('Char length should be over 0')
                                return 'CharLengthError '
                            typeList.append(temp[9:])
                            columnSet.append((column, temp[9:]))
                        else:
                            typeList.append(temp.split('\t')[1])
                            columnSet.append((column, temp.split('\t')[1]))
                    #null 여부 정보를 가져온다
                    index = str(z.pretty()).find('not')
                    if index != -1:
                        for w in z.find_data('column_name'):
                            column = str(w.pretty()).split('\t')[1].strip().lower()
                            notnullList.append(column)


        #primary key 정보를 가져온다
        primarycheck=0
        for x in items[3].find_data('table_element_list'):
            for y in x.find_data('table_element'):
                for z in y.find_data('table_constraint_definition'):
                    for w in z.find_data('primary_key_constraint'):
                        primarycheck=primarycheck+1
                        #primary key 선언이 중복되는 지 확인
                        if primarycheck>1:
                            print('Create table has failed: primary key definition is duplicated')
                            return 'DuplicatePrimaryKeyDefError'
                        else:
                            for v in w.find_data('column_name_list'):
                                for q in v.find_data('column_name'):
                                    pcolumn=str(str(q.pretty()).split('\t')[1].strip().lower())
                                    #primary key로 선언하려는 컬럼이 해당 테이블에 존재하는 지 확인
                                    if pcolumn not in columnList:
                                        print('Create table has failed: \'' + str(primary) + '\' does not exists in column definition')
                                        return 'NonExistingColumnDefError(' + str(primary) + ')'
                                    else:
                                        primaryList.append(pcolumn)

        #foreign key 정보를 가져온다
        for x in items[3].find_data('table_element_list'):
            for y in x.find_data('table_element'):
                for w in y.find_data('table_constraint_definition'):
                    for v in w.find_data('referential_constraint'):
                        tn=str(v.pretty())
                        index=tn.find('table_name')
                        tn=tn[index+11:].split('\n')[0].lower()
                        btn=bytes(tn, encoding='utf-8')
                        #참조할 테이블이 자신인지 확인(추가사항)
                        if btn==tname:
                            print('Create table has failed: foreign key references own table')
                            return 'ReferenceSelfTableError'
                        #참조할 테이블이 존재하는 지 확인
                        if not myDB.get(bytes(btn)):
                            print('Create table has failed: foreign key references non existing table')
                            return 'ReferenceTableExistenceError'
                        fList=[]
                        for k in v.find_data('column_name_list'):
                            for q in k.find_data('column_name'):
                                fList.append(str(q.pretty()).split('\t')[1].strip().lower())
                        #fList[0]은 foreign key 로 선언할 컬럼, fList[1]는 참조받을 컬럼
                        #foregin key로 선언할 컬럼이 존재하는 지 확인
                        if fList[0] not in columnList:
                            print('Create table has failed: \''+fList[0]+'\' does not exists in column definition')
                            return 'NonExistingColumnDefError('+fList[0]+')'
                        bf=bytes(fList[1], encoding='utf-8')
                        #foreign key로 참조할 컬럼이 존재하는 지 확인
                        if not myDB.get(btn+b'/'+bf):
                            print('Create table has failed: foreign key references non existing column')
                            return 'ReferenceColumnExistenceError'
                        #foreign key 가 자신 테이블에 있는 컬럼을 참조하는 지 확인(추가사항)
                        if fList[1] in columnList:
                            print('Create table has failed: foreign key references column in same table')
                            return 'ReferenceColumnSameTableError'
                        #foreign key로 참조할 컬럼이 primary key인 지 확인
                        if myDB.get(btn+b'/'+bf+b'/p')!=b'PRI':
                            print('Create table has failed: foreign key references non primary key column')
                            return 'ReferenceNonPrimaryKeyError'
                        #foreign key 참조 시 data type이 일치하는 지 확인
                        for q in range(0, len(columnList)):
                            if fList[0]==columnList[q] and typeList[q]!=myDB.get(btn+b'/'+bf+b'/t').decode():
                                print('Create table has failed: foreign key references wrong type')
                                return 'ReferenceTypeError'
                        foreignList.append((fList[0], tn, fList[1]))


        #테이블명을 db에 저장한다
        myDB.put(tname, bytes('/'.join(columnList), encoding='utf-8'))
        myDB.put(tname+b'//dn', b'0')

        #컬럼 정보를 db에 저장한다
        for i in range(len(columnList)):
            cname = bytes(columnList[i], encoding='utf-8')
            myDB.put(tname + b'/' + cname, cname)
            for (p, q) in columnSet:
                if columnList[i]==p:
                    myDB.put(tname+b'/'+cname+b'/'+b't', bytes(q, encoding='utf-8'))
            #null 정보 저장
            if columnList[i] in notnullList:
                myDB.put(tname+b'/'+cname+b'/n', b'N')
            #primary key 이면 저장, 자동으로 not null
            if columnList[i] in primaryList:
                myDB.put(tname + b'/' + cname + b'/p', b'PRI')
                myDB.put(tname + b'/' + cname + b'/n', b'N')
            #foreign key 이면 참조되는 컬럼을 값으로 저장
            for (p, q, r) in foreignList:
                if columnList[i]==p:
                    myDB.put(tname + b'/' + cname + b'/f', bytes(q, encoding='utf-8')+b'/'+bytes(r, encoding='utf-8'))

        # 참조받는 컬럼에 대한 정보를 해당 테이블에 저장한다
        for (p, q, r) in foreignList:
            myDB.put(bytes(q, encoding='utf-8') + b'/' + bytes(r, encoding='utf-8') + b'/r', tname + b'/' + bytes(p, encoding='utf-8'))


        print("\'" +tnameString+ "\'" + ' table is created')
        return 'CreateTableSuccess('+tnameString+')'

    def drop_table_query(self, items):
        print_prompt()
        tname=str(items[2].pretty()).split('\t')[1].strip().lower()
        bt=bytes(tname, encoding='utf-8')
        #삭제하려는 테이블이 존재하는 지 확인
        if myDB.get(bt):
            print('No such table')
            return 'No such table'
        #다른 테이블이 삭제하려는 테이블을 참조하는지 확인
        cursor=myDB.cursor()
        while x :=cursor.next():
            if x[0]!=bt and x[0].split(b'/')[0]==bt:
                if myDB.get(x[0].split(b'/')[0]+b'/'+x[0].split(b'/')[1]+b'/r'):
                        print('Drop table has failed: '+tname+' is referenced by other table')
                        return 'DropReferencedTableError('+tname+')'

        #테이블 삭제
        cursor=myDB.cursor()
        while x :=cursor.next():
            if x[0].split(b'/')[0]==bt:
                if x[0].split(b'/')[2]==b'f':
                    myDB.delete(x[1]+b'/r')
                myDB.delete(x[0])
        print("\'" +tname+ "\'" + ' table is deleted')
        return 'DropSuccess('+tname+')'

    def desc_query(self, items):
        print(myDB.get(b't'))
        cursor = myDB.cursor()
        while x := cursor.next():
            print(x)
        # tname=str(items[1].pretty()).split('\t')[1].strip().lower()
        # bt=bytes(tname, encoding='utf-8')
        # #테이블이 존재하는 지 확인
        # if not (myDB.get(bt)):
        #     print('No such table')
        #     return 'NoSuchTable'
        #
        # #공백 맞추어 출력하는 함수
        # def desc(x):
        #     for i in x:
        #         print('%-20s' % i, end='')
        #     print()
        #
        # print('-'*70)
        # print('table_name ['+tname+']')
        # desc(['column_name', 'type', 'null', 'key'])
        #
        # #테이블의 컬럼 모두 찾기
        # columnList=myDB.get(bt).decode().split('/')
        #
        # for x in columnList:
        #     cname=myDB.get(x).decode()
        #     type=myDB.get(x+b'/t').decode()
        #     null='Y'
        #     if myDB.get(x+b'/n'):
        #         null=myDB.get(x+b'/n').decode()
        #     pkey=''
        #     if myDB.get(x+b'/p'):
        #         pkey='PRI'
        #     fkey=''
        #     if myDB.get(x+b'/f'):
        #         fkey='FOR'
        #     if pkey and fkey:
        #         key=pkey+'/'+fkey
        #     else:
        #         key=pkey+fkey
        #     desc([cname, type, null, key])
        # print('-'*70)
        return 'DESC'

    def insert_query(self, items):
        tname=''
        for x in items[2].find_data('table_name'):
            for y in x.children:
                tname=y.value.lower()
        btname=tname.encode()

        dnum=int(myDB.get(btname+b'//dn').decode())+1

        #삽입하려는 테이블이 존재하지 않을 경우
        if not myDB.get(btname):
            print('No such table')
            return 'NoSuchTable'

        allcolumn=[]
        columnList=[]
        columnSet=[]

        for x in myDB.get(btname).decode().split('/'):
            allcolumn.append(x)

        #데이터를 삽입할 컬럼에 대한 정보를 받아온다
        if items[3].pretty().find('column_name_list')==-1:
            columnList=allcolumn
        else:
            for x in items[3].find_data('column_name'):
                for y in x.children:
                    columnList.append(y.value)

        #삽입할 데이터 정보를 타입과 데이터 쌍으로 저장한다
        for x in items[3].find_data('comparable_value'):
            for y in x.children:
                columnSet.append([y.type, y.value])

        #컬럼 수와 데이터 수가 맞지 않는 경우
        if len(columnList)!=len(columnSet):
            print('Insertion has failed: Types are not matched')
            return 'InsertTypeMismatchError'

        # primary key 제약을 위배하는 경우
        pkey = ''
        notNullList=[]
        for x in allcolumn:
            if myDB.get(btname + b'/' + x.encode() + b'/p') == b'PRI':
                pkey = x
            if myDB.get(btname + b'/' + x.encode() + b'/n')==b'N':
                notNullList.append(x)

        for x in columnSet:
            for i in range(1, dnum):
                if x[1]==myDB.get(btname+b'/'+pkey.encode()+b'/'+str(i).encode()).decode():
                    print('Insertion has failed: Primary key duplication')
                    return 'InsertDuplicatePrimaryKeyError'

        #null 값을 가질 수 없는 컬럼을 제외하고 삽입하려는 경우
        for x in notNullList:
            if x not in columnList:
                print('Insertion has failed: '+x+' is not nullable')
                return 'InsertColumnNonNullableError'

        def typeError():
            print('Insertion has failed: Types are not matched')
            return 'InsertTypeMismatchError'

        for i in range(0, len(columnList)):
            #존재하지 않는 컬럼에 값을 삽입하려는 경우
            if not myDB.get(btname+b'/'+columnList[i].encode()):
                print('Insertion has failed: \''+columnList[i]+'\' does not exist')
                return 'InsertColumnExistenceError'
            #null 값 가질 수 없는 컬럼에 null 값을 삽입하려는 경우
            if columnSet[i][0]=='NULL':
                if myDB.get(btname+b'/'+columnList[i].encode()+b'/n')==b'N':
                    print('Insertion has failed: '+columnList[i]+' is not nullable')
                    return 'InsertColumnNonNullableError'
            # 데이터의 타입이 컬럼의 타입과 맞지 않는 경우
            if columnSet[i][0]=='STR':
                if myDB.get(btname+b'/'+columnList[i].encode()+b'/t').decode().find('char')==-1:
                    if typeError():
                        return
            else:
                if myDB.get(btname+b'/'+columnList[i].encode()+b'/t').decode()!=columnSet[i][0].lower():
                    if typeError():
                        return
            #foreign key 제약을 위배하는 경우
            ftemp=myDB.get(btname+b'/'+columnList[i].encode()+b'/f')
            if not ftemp:
                continue
            else:
                check=0
                fdnum=int(myDB.get(ftemp.split(b'/')[0]+b'//dn').decode())
                for k in range(1,fdnum+1):
                    if columnSet[i][0]!='NULL' and myDB.get(ftemp+b'/'+str(k).encode()).decode()==columnSet[i][1]:
                        check=1
                        break
                if not check:
                    print('Insertion has failed: Referential integrity violation')
                    return 'InsertReferentialIntegrityError'

        #insert 실행
        for x in allcolumn:
            if x in columnList:
                i=columnList.index(x)
                if columnSet[i][0]=='NULL':
                    myDB.put(btname + b'/' + x.encode() + b'/' + str(dnum).encode(), b' ')
                #지정된 char 길이만큼만 잘라서 저장
                elif columnSet[i][0]=='STR':
                    temp=myDB.get(btname+b'/'+x.encode()+b'/t').decode()
                    clen=int(temp[temp.find('(')+1: temp.find(')')])
                    myDB.put(btname+b'/'+x.encode()+b'/'+str(dnum).encode(), columnSet[i][1][:clen].encode())
                else:
                    myDB.put(btname + b'/' + x.encode() + b'/' + str(dnum).encode(), columnSet[i][1].encode())
            else:
                myDB.put(btname+b'/'+x.encode()+b'/'+str(dnum).encode(), b' ')

        #table 데이터 수 정보 갱신
        myDB.delete(btname+b'//dn')
        myDB.put(btname+b'//dn', str(dnum).encode())

        print_prompt()
        print('The row is inserted')
        return 'INSERT'

    def delete_query(self, items):

        orList = []
        columnList = []
        table=''

        # from 절 해석
        for x in items[2].find_data('table_name'):
            for y in x.children:
                table=y.value

        btname=table.encode()

        # from 구문의 테이블이 존재하는 지 확인
        if not myDB.get(btname):
            print('No such table')
            return 'NoSuchTable'
        # where 절 해석
        try:
            for x in items[3].find_data('boolean_term'):
                andList = []
                for y in x.find_data('boolean_factor'):
                    cvalue = ''
                    column = []
                    valueL=[]
                    ntable = ''
                    ncol = ''
                    op = ''
                    code = 0
                    notcheck = 1
                    for z in y.children:
                        try:
                            if z.value:
                                notcheck = -1
                        except:
                            pass

                    if y.pretty().find('comparison_predicate') != -1:

                        for z in y.find_data('comp_operand'):

                            table = ''
                            col = ''
                            for v in z.find_data('comparable_value'):
                                for w in v.children:
                                    ctype = w.type
                                    cvalue = w.value

                            for v in z.find_data('table_name'):
                                for w in v.children:
                                    table = w.value

                            for v in z.find_data('column_name'):
                                for w in v.children:
                                    col = w.value

                            column.append([table, col])
                            valueL.append(([ctype, cvalue]))


                        for z in y.find_data('comparison_predicate'):
                            op = z.children[1].value

                    else:
                        for z in y.find_data('table_name'):
                            for v in z.children:
                                ntable = v.value
                        for z in y.find_data('column_name'):
                            for v in z.children:
                                ncol = v.value
                        for z in y.find_data('null_operation'):
                            op = z.children[1].value
                        code = 4
                    if code == 4:
                        andList.append([code * notcheck, [ntable, ncol], op])
                    if (not column[0][1]) and (not column[1][1]):
                        andList.append([5*notcheck,[valueL[0][0], valueL[0][1]], [valueL[1][0], valueL[1][1]],op])
                    elif not column[1][1]:
                        andList.append([2 * notcheck, [column[0][0], column[0][1]], [valueL[1][0], valueL[1][1]], op])
                    elif not column[0][1]:
                        andList.append([3 * notcheck, [valueL[0][0], valueL[0][1]], [column[1][0], column[1][1]], op])
                    else:
                        andList.append([1 * notcheck, [column[0][0], column[0][1]], [column[1][0], column[1][1]], op])

                orList.append(andList)
        except:
            pass

        #from의 테이블에 있는 컬럼 이름들을 가져온다
        for x in myDB.get(btname).decode().split('/'):
            columnList.append(x)

        # where 문의 컬럼 참조, 테이블 참조가 올바른 지 확인하는 함수
        def columncheck(a):
            checkList = []
            if a[0]:
                check = 0
                ascol = ''
                if a[0]==table:
                    check = 1
                if not check:
                    print('Where clause try to reference tables which are not specified')
                    return 'WhereTableNotSpecified'
                if ascol:
                    if not myDB.get(ascol.encode() + b'/' + a[1].encode()):
                        print('Where clause try to reference non existing column')
                        return 'WhereColumnNotExist'
                elif not myDB.get(a[0].encode() + b'/' + a[1].encode()):
                    print('Where clause try to reference non existing column')
                    return 'WhereColumnNotExist'
            else:
                temp = myDB.get(btname + b'/' + a[1].encode())
                if temp:
                    checkList.append(temp)
                if len(checkList) == 0:
                    print('Where clause try to reference non existing column')
                    return 'WhereColumnNotExist'
                if len(checkList) > 1:
                    print('Where clause contains ambiguous reference')
                    return 'WhereAmbiguousReference'

        # 컬럼이 속해 있는 테이블을 찾는 함수
        def tfind(a):
            if not a[0]:
                if myDB.get(btname + b'/' + a[1].encode()):
                    return table
            else:
                return a[0]

        def compare(x, y, c, d):
            if x==' ' or y==' ':
                return 0

            if d == 'int':
                a = int(x)
                b = int(y)
            else:
                a = x
                b = y

            if c == '>':
                if a > b:
                    return 1
                else:
                    return 0
            if c == '<':
                if a < b:
                    return 1
                else:
                    return 0
            if c == '=':
                if a == b:
                    return 1
                else:
                    return 0
            if c == '<=':
                if a < b:
                    return 1

                if a == b:
                    return 1
                else:
                    return 0
            if c == '>=':
                if a >= b:
                    return 1
                else:
                    return 0
            else:
                if a != b:
                    return 1
                else:
                    return 0

        def typeError():
            print('Where clause try to compare incomparable values')
            return 'WhereIncomparableError'

        # 옳은 where 구문인지 확인하고 출력할 값의 데이터 번호를 answer에 저장한다
        answer = []
        for x in orList:
            andanswer = []
            andindex = 0
            for y in x:
                smallanswer = []
                if abs(y[0]) == 1:
                    if columncheck(y[1]):
                        return
                    if columncheck(y[2]):
                        return
                    t1 = myDB.get(btname + b'/' + y[1][1].encode() + b'/t').decode()
                    t2 = myDB.get(btname + b'/' + y[2][1].encode() + b'/t').decode()
                    if t1 == t2 or ('char' in t1 and 'char' in t2):
                        if y[0] > 0:
                            for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                if compare(myDB.get(btname + b'/' + y[1][1].encode() + b'/' + str(i).encode()).decode(), myDB.get(btname + b'/' + y[2][1].encode() + b'/' + str(j).encode()).decode(), y[3], t1.lower()):
                                    smallanswer.append(i)
                        else:
                            for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                if not compare(myDB.get(btname + b'/' + y[1][1].encode() + b'/' + str(i).encode()).decode(), myDB.get(btname + b'/' + y[2][1].encode() + b'/' + str(j).encode()).decode(), y[3], t1.lower()):
                                    smallanswer.append(i)

                    else:
                        if typeError():
                            return

                if abs(y[0]) == 2:
                    if columncheck(y[1]):
                        return
                    t = myDB.get(btname + b'/' + y[1][1].encode() + b'/t').decode()
                    if t == y[2][0].lower() or (y[2][0].lower() == 'str' and 'char' in t):
                        if y[0] > 0:
                            for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                if compare(myDB.get(btname + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()).decode(), y[2][1], y[3], t.lower()):
                                    smallanswer.append(i)

                        else:
                            for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                if not compare(myDB.get(btname + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()).decode(), y[2][1], y[3], t.lower()):
                                    smallanswer.append(i)
                    else:
                        if typeError():
                            return
                if abs(y[0]) == 3:
                    if columncheck(y[2]):
                        return
                    t = myDB.get(btname + b'/' + y[2][1].encode() + b'/t').decode()
                    if t == y[1][0].lower() or (y[1][0].lower() == 'str' and 'char' in t):
                        if y[0] > 0:
                            for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                if compare(myDB.get(btname + b'/' + y[2][1].encode() + b'/' + str(
                                        i).encode()).decode(), y[1][1], y[3], t.lower()):
                                    smallanswer.append(i)
                        else:
                            for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                if not compare(myDB.get(btname + b'/' + y[2][1].encode() + b'/' + str(
                                        i).encode()).decode(), y[1][1], y[3], t.lower()):
                                    smallanswer.append(i)
                    else:
                        if typeError():
                            return
                if abs(y[0]) == 4:
                    if columncheck(y[1]):
                        return
                    if y[0]>0:
                        for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                            if y[2].lower()=='not':
                                if myDB.get(btname+b'/'+y[1][1].encode()+b'/'+str(i).encode())!=b' ':
                                    smallanswer.append(i)
                            if y[2].lower()=='null':
                                if myDB.get(tfind(y[1]).encode()+b'/'+y[1][1].encode()+b'/'+str(i).encode())==b' ':
                                    smallanswer.append(i)

                    else:
                        for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                            if y[2].lower() == 'not':
                                if myDB.get(btname + b'/' + y[1][1].encode() + b'/' + str(i).encode()) == b' ':
                                    smallanswer.append(i)
                            if y[2].lower() == 'null':
                                if myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()) != b' ':
                                    smallanswer.append(i)
                if abs(y[0])==5:
                    t1 = y[1][0]
                    t2 = y[2][0]
                    if t1==t2:
                        if y[0]>0:
                            if compare(y[1][1], y[2][1], y[3], t1.lower()):
                                for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                    smallanswer.append(i)
                        else:
                            if not compare(y[1][1], y[2][1], y[3], t1.lower()):
                                for i in range(1, int(myDB.get(btname + b'//dn').decode()) + 1):
                                    smallanswer.append(i)
                    else:
                        if typeError():
                            return

                andcopy = []
                if not andindex:
                    for z in smallanswer:
                        andanswer.append(z)
                else:
                    for z in smallanswer:
                        if z in andanswer:
                            andcopy.append(z)
                    andanswer = andcopy
                andindex = andindex + 1
            for z in andanswer:
                answer.append(z)

        #where 구문이 없는 경우
        if not orList:
            for i in range(1, int(myDB.get(btname+b'//dn').decode())+1):
                answer.append(i)

        #데이터 삭제 시 중복 열 번호 제거
        mySet=set(answer)
        answer=list(mySet)
        answer.sort()
        imp=[]

        # referential integrity 해결 함수
        def refdelsearch(t, x, index, impindex):
            rt = myDB.get(t + b'/' + x.encode() + b'/r')
            if rt:
                for i in range(1, int(myDB.get(rt.split(b'/')[0] + b'//dn').decode()) + 1):
                    # 지우려는 값이 참조받고 있는 지 확인
                    if myDB.get(rt + b'/' + str(i).encode()) == myDB.get(
                            t + b'/' + x.encode() + b'/' + str(index).encode()):
                        if myDB.get(rt + b'/n') == b'N':
                            imp.append(impindex)
                            impset = set(imp)
                            imp = list(impset)
                            return 0
                        else:
                            ttemp = rt.split(b'/')[0]
                            tt = rt.split(b'/')[1]
                            changenull = refdelsearch(ttemp, tt.decode(), i, impindex)
                            return changenull
            return 1

        def refdel(t, x, index, impindex):
            rt = myDB.get(t + b'/' + x.encode() + b'/r')
            if rt:
                for i in range(1, int(myDB.get(rt.split(b'/')[0] + b'//dn').decode()) + 1):
                    # 지우려는 값이 참조받고 있는 지 확인
                    if myDB.get(rt + b'/' + str(i).encode()) == myDB.get(
                            t + b'/' + x.encode() + b'/' + str(index).encode()):
                        if myDB.get(rt + b'/n') == b'N':
                            return 0
                        else:
                            ttemp = rt.split(b'/')[0]
                            tt = rt.split(b'/')[1]
                            changenull = refdel(ttemp, tt.decode(), i, impindex)
                            if changenull:
                                myDB.delete(rt + b'/' + str(i).encode())
                                myDB.put(rt + b'/' + str(i).encode(), b' ')
                            return changenull
            return 1

        # delete 실행
        dnum = int(myDB.get(btname + b'//dn').decode())
        delAnswer = []
        print(orList)
        print(answer)
        for x in answer:
            pc = 1
            for y in columnList:
                pc = pc * refdelsearch(btname, y, x, x)
            if pc:
                if not (x in imp):
                    for y in columnList:
                        refdel(btname, y, x, x)
                        myDB.delete(btname + b'/' + y.encode() + b'/' + str(x).encode())
                    delAnswer.append(x)

        # 지워진 데이터 자리를 메우는 재배열
        for i in range(len(delAnswer)):
            for j in range(delAnswer[i], dnum - i):
                for x in columnList:
                    puttemp = myDB.get(btname + b'/' + x.encode() + b'/' + str(j + 1).encode())
                    print(puttemp is None)
                    if puttemp is None:
                        break
                    else:
                        myDB.put(btname + b'/' + x.encode() + b'/' + str(j).encode(), puttemp)
                        myDB.delete(btname + b'/' + x.encode() + b'/' + str(j + 1).encode())
                try:
                    myDB.delete(btname + b'/' + x.encode() + b'/' + str(dnum - i).encode())
                except:
                    pass

        #테이블에 존재하는 값 개수 수정
        myDB.delete(btname + b'//dn')
        myDB.put(btname + b'//dn', str(dnum-len(delAnswer)).encode())

        print_prompt()
        print(len(delAnswer),'row(s) are deleted')
        return 'DeleteResult'

    def select_query(self, items):
        orList=[]
        columnList=[]
        tableList=[]
        allcolumn=0

        #column 정보 받아오기
        for x in items[1].find_data('select_list'):
            if not x.children:
                allcolumn=1
                break

            for y in x.find_data('selected_column'):
                table = ''
                col = ''
                asas=''
                for z in y.find_data('table_name'):
                    for v in z.children:
                        table = v.value

                for z in y.find_data('column_name'):
                    for v in z.children:
                        if not col:
                            col = v.value
                        else:
                            asas=v.value
                columnList.append([table, col, asas])


        #from 절 해석
        for x in items[2].find_data('referred_table'):
            table = ''
            asas=''
            for y in x.find_data('table_name'):
                for v in y.children:
                    if not table:
                        table = v.value
                    else:
                        asas=v.value
            tableList.append([table, asas])

        #where 절 해석
        for x in items[2].find_data('boolean_term'):
            andList=[]
            for y in x.find_data('boolean_factor'):
                cvalue = ''
                column = []
                valueL=[]
                ntable=''
                ncol=''
                op=''
                code=0
                notcheck=1
                for z in y.children:
                    try:
                        if z.value:
                            notcheck=-1
                    except:
                        pass

                if y.pretty().find('comparison_predicate')!=-1:
                    for z in y.find_data('comp_operand'):
                        ctype=''
                        cvalue=''
                        table = ''
                        col = ''
                        for v in z.find_data('comparable_value'):
                            for w in v.children:
                                ctype=w.type
                                cvalue=w.value

                        for v in z.find_data('table_name'):
                            for w in v.children:
                                table=w.value

                        for v in z.find_data('column_name'):
                            for w in v.children:
                                col=w.value

                        column.append([table, col])
                        valueL.append([ctype, cvalue])

                    for z in y.find_data('comparison_predicate'):
                        op=z.children[1].value

                else:
                    for z in y.find_data('table_name'):
                        for v in z.children:
                            ntable=v.value
                    for z in y.find_data('column_name'):
                        for v in z.children:
                            ncol=v.value
                    for z in y.find_data('null_operation'):
                        op=z.children[1].value
                    code=4

                if code==4:
                    andList.append([code*notcheck, [ntable, ncol], op])
                if (not column[0][1]) and (not column[1][1]):
                    andList.append([5 * notcheck, [valueL[0][0], valueL[0][1]], [valueL[1][0], valueL[1][1]], op])
                elif not column[1][1]:
                    andList.append([2 * notcheck, [column[0][0], column[0][1]], [valueL[1][0], valueL[1][1]], op])
                elif not column[0][1]:
                    andList.append([3 * notcheck, [valueL[0][0], valueL[0][1]], [column[1][0], column[1][1]], op])
                else:
                    andList.append([1 * notcheck, [column[0][0], column[0][1]], [column[1][0], column[1][1]], op])
            orList.append(andList)

        nowhere = 0
        if not orList:
            nowhere=1

        # from 구문의 테이블이 존재하는 지 확인
        for x in tableList:
            if not myDB.get(x[0].encode()):
                print('Selection has failed: \'' + x[0] + '\' does not exist')
                return 'SelectTableExistenceError(' + x[0] + ')'

        #컬럼 select 가 올바른 지 확인
        for x in columnList:
            colcheck = []
            if not x[0]:
                for y in tableList:
                    temp=myDB.get(y[0].encode()+b'/'+x[1].encode())
                    if temp:
                        tem=y[0]+'/'+temp.decode()
                        colcheck.append(tem)
            else:
                temp = myDB.get(x[0].encode() + b'/' + x[1].encode())
                if temp:
                    colcheck.append([0].encode() + b'/' + x[1].encode())
                else:
                    print('Selection has failed: fail to resolve \'' + x[0]+'.' + x[1] + '\'')
                    return 'SelectColumnResolveError(' + x[1] + ')'
            if len(colcheck)!=1:
                print('Selection has failed: fail to resolve \'' + x[1] + '\'')
                return 'SelectColumnResolveError(' + x[1] + ')'

        # 모든 컬럼을 출력하고자 할 때
        if allcolumn == 1:
            for x in tableList:
                for y in myDB.get(x[0].encode()).decode().split('/'):
                    columnList.append([x[0], y,''])

        #where 문의 컬럼 참조, 테이블 참조가 올바른 지 확인하는 함수
        def columncheck(a):
            checkList = []
            if a[0]:
                check = 0
                ascol=''
                for [p, q] in tableList:
                    if p == a[0]:
                        check=1
                    if q == a[0]:
                        ascol=p
                        check = 1
                if not check:
                    print('Where clause try to reference tables which are not specified')
                    return 'WhereTableNotSpecified'
                if ascol:
                    if not myDB.get(ascol.encode()+b'/'+a[1].encode()):
                        print('Where clause try to reference non existing column')
                        return 'WhereColumnNotExist'
                elif not myDB.get(a[0].encode()+b'/'+a[1].encode()):
                    print('Where clause try to reference non existing column')
                    return 'WhereColumnNotExist'
            else:
                for [p, q] in tableList:
                    temp=myDB.get(p.encode()+b'/'+a[1].encode())
                    if temp:
                        checkList.append(temp)
                if len(checkList)==0:
                    print('Where clause try to reference non existing column')
                    return 'WhereColumnNotExist'
                if len(checkList)>1:
                    print('Where clause contains ambiguous reference')
                    return 'WhereAmbiguousReference'

        #컬럼이 속해 있는 테이블을 찾는 함수
        def tfind(a):
            if not a[0]:
                for [p, q] in tableList:
                    if myDB.get(p.encode()+b'/'+a[1].encode()):
                        return p
            else:
                return a[0]

        def compare(x,y,c,d):
            if x == ' ' or y == ' ':
                return 0

            if d=='int':
                a=int(x)
                b=int(y)
            else:
                a=x
                b=y

            if c=='>':
                if a>b:
                    return 1
                else:
                    return 0
            if c=='<':
                if a<b:
                    return 1
                else:
                    return 0
            if c=='=':
                if a==b:
                    return 1
                else:
                    return 0
            if c=='<=':
                if a<b:
                    return 1

                if a==b:
                    return 1
                else:
                    return 0
            if c=='>=':
                if a>=b:
                    return 1
                else:
                    return 0
            else:
                if a!=b:
                    return 1
                else:
                    return 0

        def typeError():
            print('Where clause try to compare incomparable values')
            return 'WhereIncomparableError'

        # 옳은 where 구문인지 확인하고 출력할 값을 answer에 저장한다

        print('a')
        answer = []
        for x in orList:
            andanswer = []
            andindex = 0
            for y in x:
                smallanswer = []
                if abs(y[0]) == 1:
                    if columncheck(y[1]):
                        return
                    if columncheck(y[2]):
                        return
                    t1 = myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/t').decode()
                    t2 = myDB.get(tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/t').decode()
                    if t1 == t2 or ('char' in t1 and 'char' in t2):
                        if y[0] > 0:
                            if tfind(y[1]) != tfind(y[2]):
                                for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                                    for j in range(1, int(myDB.get(tfind(y[2]).encode() + b'//dn').decode()) + 1):
                                        andtemp = []
                                        if compare(myDB.get(
                                                tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                                        i).encode()).decode(), myDB.get(
                                            tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/' + str(
                                                j).encode()).decode(), y[3], t1.lower()):
                                            for k in columnList:
                                                if tfind(k) == tfind(y[1]):
                                                    andtemp.append(
                                                        tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(
                                                            i).encode())
                                                elif tfind(k) == tfind(y[2]):
                                                    andtemp.append(
                                                        tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(
                                                            j).encode())
                                                else:
                                                    andtemp.append(b'')
                                        smallanswer.append(andtemp)
                            else:
                                for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                                    for j in range(1, i):
                                        andtemp = []
                                        if compare(
                                                myDB.get(
                                                    tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                                        i).encode()).decode(),
                                                myDB.get(
                                                    tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/' + str(
                                                        j).encode()).decode(),
                                                y[3], t1.lower()):
                                            for k in columnList:
                                                if tfind(k) == tfind(y[1]):
                                                    andtemp.append(
                                                        tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(
                                                            i).encode())
                                                else:
                                                    andtemp.append(b'')
                                        smallanswer.append(andtemp)
                        else:
                            if tfind(y[1]) != tfind(y[2]):
                                for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                                    for j in range(1, int(myDB.get(tfind(y[2]).encode() + b'//dn').decode()) + 1):
                                        andtemp = []
                                        if not compare(myDB.get(
                                                tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                                    i).encode()).decode(), myDB.get(
                                            tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/' + str(
                                                j).encode()).decode(), y[3], t1.lower()):
                                            for k in columnList:
                                                if tfind(k) == tfind(y[1]):
                                                    andtemp.append(
                                                        tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(
                                                            i).encode())
                                                elif tfind(k) == tfind(y[2]):
                                                    andtemp.append(
                                                        tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(
                                                            j).encode())
                                                else:
                                                    andtemp.append(b'')
                                        smallanswer.append(andtemp)
                            else:
                                for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                                    for j in range(1, int(myDB.get(tfind(y[2]).encode() + b'//dn').decode()) + 1):
                                        andtemp = []
                                        if not compare(
                                                myDB.get(
                                                    tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                                        i).encode()).decode(),
                                                myDB.get(
                                                    tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/' + str(
                                                        j).encode()).decode(),
                                                y[3], t1.lower()):
                                            for k in columnList:
                                                if tfind(k) == tfind(y[1]):
                                                    andtemp.append(
                                                        tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(
                                                            i).encode())
                                                else:
                                                    andtemp.append(b'')
                                        smallanswer.append(andtemp)
                    else:
                        if typeError():
                            return

                if abs(y[0]) == 2:
                    if columncheck(y[1]):
                        return
                    t = myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/t').decode()
                    if t == y[2][0].lower() or (y[2][0].lower() == 'str' and 'char' in t):
                        if y[0] > 0:
                            for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                                andtemp = []
                                if compare(myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()).decode(), y[2][1], y[3], t.lower()):
                                    for k in columnList:
                                        if tfind(k) == tfind(y[1]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                                smallanswer.append(andtemp)
                        else:
                            for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                                andtemp = []
                                if not compare(myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()).decode(), y[2][1], y[3], t.lower()):
                                    for k in columnList:
                                        if tfind(k) == tfind(y[1]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                                smallanswer.append(andtemp)
                    else:
                        if typeError():
                            return
                if abs(y[0]) == 3:
                    if columncheck(y[2]):
                        return
                    t = myDB.get(tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/t').decode()
                    if t == y[1][0].lower() or (y[1][0].lower() == 'str' and 'char' in t):
                        if y[0] > 0:
                            for i in range(1, int(myDB.get(tfind(y[2]).encode() + b'//dn').decode()) + 1):
                                andtemp = []
                                if compare(myDB.get(
                                        tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/' + str(
                                            i).encode()).decode(),
                                           y[1][1], y[3], t.lower()):
                                    for k in columnList:
                                        if tfind(k) == tfind(y[2]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                                smallanswer.append(andtemp)
                        else:
                            for i in range(1, int(myDB.get(tfind(y[2]).encode() + b'//dn').decode()) + 1):
                                andtemp = []
                                if not compare(myDB.get(
                                        tfind(y[2]).encode() + b'/' + y[2][1].encode() + b'/' + str(
                                            i).encode()).decode(),
                                               y[1][1], y[3], t.lower()):
                                    for k in columnList:
                                        if tfind(k) == tfind(y[2]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                                smallanswer.append(andtemp)
                    else:
                        if typeError():
                            return
                if abs(y[0]) == 4:
                    if columncheck(y[1]):
                        return
                    if y[0]> 0:
                        for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                            andtemp = []
                            if y[2].lower() == 'not':
                                print('a')
                                if myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()) != b' ':
                                    print('b')
                                    for k in columnList:
                                        print('c')
                                        if tfind(k) == tfind(y[1]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                            if y[2].lower == 'null':
                                if myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()) == b' ':
                                    for k in columnList:
                                        if tfind(k) == tfind(y[1]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                            smallanswer.append(andtemp)

                    else:
                        for i in range(1, int(myDB.get(tfind(y[1]).encode() + b'//dn').decode()) + 1):
                            andtemp = []
                            if y[2].lower == 'not':
                                if myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()) == b' ':
                                    for k in columnList:
                                        if tfind(k) == tfind(y[1]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                            if y[2].lower == 'null':
                                if myDB.get(tfind(y[1]).encode() + b'/' + y[1][1].encode() + b'/' + str(
                                        i).encode()) != b' ':
                                    for k in columnList:
                                        if tfind(k) == tfind(y[1]):
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        else:
                                            andtemp.append(b'')
                            smallanswer.append(andtemp)
                if abs(y[0])==5:
                    print('go')
                    t1 = y[1][0]
                    t2 = y[2][0]
                    print(y)
                    if t1==t2:
                        if y[0]>0:
                            if compare(y[1][1], y[2][1], y[3], t1.lower()):
                                print('c')
                                for z in tableList:
                                    for i in range(1, int(myDB.get(z[0].encode() + b'//dn').decode()) + 1):
                                        andtemp = []
                                        for k in columnList:
                                            print(k)
                                            andtemp.append(tfind(k).encode()+b'/'+k[1].encode()+b'/'+str(i).encode())
                                        smallanswer.append(andtemp)
                        else:
                            if not compare(y[1][1], y[2][1], y[3], t1.lower()):

                                for z in tableList:
                                    for i in range(1, int(myDB.get(z[0].encode() + b'//dn').decode()) + 1):
                                        andtemp = []
                                        for k in columnList:
                                            print(k)
                                            andtemp.append(
                                                tfind(k).encode() + b'/' + k[1].encode() + b'/' + str(i).encode())
                                        smallanswer.append(andtemp)
                    else:
                        if typeError():
                            return

                andcopy = []
                if not andindex:
                    for z in smallanswer:
                        andanswer.append(z)
                else:
                    for z in smallanswer:
                        if z in andanswer:
                            andcopy.append(z)
                    andanswer = andcopy
                andindex = andindex + 1
            for z in andanswer:
                answer.append(z)

        # select 출력
        #컬럼 이름 줄 출력
        print('+', end='')
        for x in columnList:
            print('-'*(len(x[1])+2)+'+',end='')
        print('\n|', end='')
        for x in columnList:
            if x[2]:
                print(' '+x[2], end=' |')
            else:
                print(' ' + x[1], end=' |')
        print('\n+',end='')
        for x in columnList:
            print('-'*(len(x[1])+2)+'+',end='')
        print()
        #where 조건 만족하는 값 출력
        if nowhere:
            if len(tableList)==1:
                for i in range(1, int(myDB.get(tableList[0][0].encode()+b'//dn').decode())+1):
                    for x in columnList:
                        tn=0
                        if x[2]:
                            tn = len(x[2]) + 2
                        else:
                            tn = len(x[1]) + 2
                        pp = myDB.get(tableList[0][0].encode()+b'/'+x[1].encode()+b'/'+str(i).encode()).decode()
                        print('|', end='')
                        print(pp.center(tn, ' '), end='')
                    print('|')

            if len(tableList)==2:
                for x in range(1, int(myDB.get(tableList[0][0].encode()+b'//dn').decode())+1):
                    for y in range(1, int(myDB.get(tableList[1][0].encode()+b'//dn').decode())+1):
                        for z in columnList:
                            tn=0
                            if z[2]:
                                tn=len(z[2])+2
                            else:
                                tn=len(z[1])+2

                            if tfind(z) == tableList[0][0]:
                                pp = myDB.get(tableList[0][0].encode()+b'/'+z[1].encode()+b'/'+str(x).encode()).decode()
                                print('|', end='')
                                print(pp.center(tn, ' '), end='')

                            elif tfind(z) == tableList[1][0]:
                                pp = myDB.get(
                                    tableList[1][0].encode() + b'/' + z[1].encode() + b'/' + str(y).encode()).decode()
                                print('|', end='')
                                print(pp.center(tn, ' '), end='')
                        print('|')

        else:
            for x in answer:
                i=0
                for y in x:
                    tn=0
                    if columnList[i][2]:
                        tn=len(columnList[i][2])+2
                    else:
                        tn=len(columnList[i][1])+2
                    pp=myDB.get(y).decode()
                    print('|', end='')
                    print(pp.center(tn,' '),end='')
                i+=1
                print('|')
        print('+', end='')
        for x in columnList:
            print('-'*(len(x[1])+2)+'+',end='')
        print()


        return 'SELECT'

    def show_table_query(self, items):
        print('-'*20)
        cursor=myDB.cursor()
        while x := cursor.next():
            if len(x[0].decode().split('/'))==1:
                print(x[0].decode().split('/')[0])
        print('-'*20)
        return 'SHOW TABLES'

#정의한 parser를 불러온다.
with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start='command', lexer='standard', parser='lalr', transformer=MyTransformer())



while True:

    # 프롬프트 출력하고 쿼리 입력을 받기 위한 부분
    command = str("")
    print_prompt()
    while True:
        command += input()

        #입력 문자가 없으면 프롬프트를 출력한다
        if not command.strip():
            break
        elif command.rstrip()[-1] == ';':
            command=command.strip()
            break

        # 쿼리를 ';'로 마치지 않고 엔터 칠 시 띄어쓰기로 간주하고 다음 줄에 이어서 입력 받는다
        else:
            command+=' '

    # 여러 쿼리가 들어올 경우 ';'로 구분하여 처리한다
    #';'로 끝나는 문자열을 ';'을 기준 삼아 split()으로 나눌 경우 마지막에 ''가 추가되는 문제 해결
    splitCommand=command.split(';')
    del splitCommand[-1]

    for i in splitCommand:

        # exit 입력 시 종료한다
        if i.strip().lower() == 'exit':
             print('Bye')
             myDB.close()
             exit()
        try:
            result = sql_parser.parse(i.strip()+';')
        except:
            print_prompt()
            print('Syntax error')
            break
