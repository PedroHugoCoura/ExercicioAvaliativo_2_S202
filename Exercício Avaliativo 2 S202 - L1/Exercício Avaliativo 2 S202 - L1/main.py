# from pprintpp import pprint as pp
from db.database import Graph
from helper.write_a_json import write_a_json as wj

db = Graph(uri='bolt://54.163.142.122:7687', user='neo4j', password='masters-halts-deposition')
# Questão 01
# A
aux = db.execute_query("match(t:Teacher{name:'Renzo'}) return t.ano_nasc, t.cpf")
wj(aux, '1A')

# B
aux = db.execute_query("match(t:Teacher) where t.name=~'M.*' return t.name, t.cpf")
wj(aux, '1B')

# C
aux = db.execute_query("match(c:City) return c.name")
wj(aux, '1C')

# D
aux = db.execute_query("match(s:School) where s.number>=150 OR s.number<=550 return s.name, s.address, s.number")
wj(aux, '1D')

# Questão 02
# A
aux = db.execute_query("match(t:Teacher) return MAX(t.ano_nasc), MIN(t.ano_nasc)")
wj(aux, '2A')

# B
aux = db.execute_query("match(c:City) return AVG(c.population)")
wj(aux, '2B')

# C
aux = db.execute_query("match(c:City{cep:'37540-000'}) return REPLACE(c.name, 'a', 'A')")
wj(aux, '2C')

# D
aux = db.execute_query("match(t:Teacher) return substring(t.name, 3, 1)")
wj(aux, '2D')

# Questão 03
# A
class TeacherCRUD(object):
    def __init__(self):
        self.db = Graph(uri='bolt://54.163.142.122:7687', user='neo4j', password='masters-halts-deposition')

    def create(self, teacher):
        aux = self.db.execute_query('create (t:Teacher {name:$name, ano_nasc:$ano_nasc, cpf:$cpf}) return t',
                                     {'name': teacher['name'], 'ano_nasc': teacher['ano_nasc'], 'cpf': teacher['cpf']})
        wj(aux, 'create')
        return aux

    def read(self, teacher):
        aux = self.db.execute_query('match (t:Teacher {name:$name}) return t',
                                     {'name': teacher['name']})
        wj(aux, 'read')
        return aux

    def update(self, teacher):
        aux = self.db.execute_query('match (t:Teacher {name:$name}) set t.cpf = $cpf return t',
                                     {'name': teacher['name'], 'cpf': teacher['cpf']})
        wj(aux, 'update')
        return aux

    def delete(self, teacher):
        aux = self.db.execute_query('match (t:Teacher {name:$name}) delete t',
                                     {'name': teacher['name']})
        wj(aux, 'delete')
        return aux

aux = TeacherCRUD()

# B
teacher = {
    'name': 'Chris Lima',
    'ano_nasc': 1956,
    'cpf': '189.052.396-66'
}
aux.create(teacher)

# C
teacher = {
    'name': 'Chris Lima'
}
aux.read(teacher)

# D
teacher = {
    'name': 'Chris Lima',
    'cpf': '162.052.777-77'
}
aux.update(teacher)

aux.db.close()

