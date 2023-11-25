import re
from sys import argv
import random
import unittest


mutations = []
method_dict = {}
module_code = {}

mutants_created = 0
mutants_killed = 0

def write_mutation(module, ln, change):
    with open(module, 'w') as original_file:
        for index, line in enumerate(module_code[module]):
            if index == ln:
                original_file.write(change)
            else:
                original_file.write(line)

def write_file_back(module):
    with open(module, 'w') as original_file:
        for index, line in enumerate(module_code[module]):
            original_file.write(line)

def get_function_names(module):
    pattern = r"def\s+(\w+)\(([^)]*)\)\s*->\s*(\w+):"
    f = open(module, "r")
    for line in f:
        if line.strip().startswith('def'):
            match = re.match(pattern, line)

            function_name = match.group(1)
            return_type = match.group(3)
            parameters_with_types = match.group(2)
            param_type_pairs = [param.strip() for param in parameters_with_types.split(',')]

            params = []
            for param_pair in param_type_pairs:
                if ':' in param_pair:
                    name, param_type = [part.strip() for part in param_pair.split(':')]
                    params.append((name, param_type))
                else:
                    params.append((name, None))

            method_dict[function_name] = [return_type, params]

def get_function_from_line(line):
    pattern = r"def\s+(\w+)\(([^)]*)\)\s*->\s*(\w+):"
    match = re.match(pattern, line)
    function_name = match.group(1)
    return function_name

def substitute(param_value, param_type):
    if param_type == 'bool':
        if (param_value == 'True'):
            return 'False'
        elif param_value == 'False':
            return 'True'
        else:
            return random.choice(['True', 'False'])
        
    elif param_type == 'int':
        return str(random.randint(-1000, 1000)) 
        
    elif param_type == 'str':
        return '""'
    
    elif param_type == 'list':
        return '[]'
    
    elif param_type == 'dict':
        return '{}'
    
    else:
        return param_value


def mutate_function_calls(module):
    pattern = r"(\w+)\(([^)]*)\)"
    pattern1 = r"=\s+(\w+)\(([^)]*)\)"

    f = open(module, "r")
    method = ""
    for idx, line in enumerate(f):

        if (line.strip().startswith("def")):
            method = get_function_from_line(line)
        elif (method != "" and (line.startswith(" "))):
            for key in list(method_dict.keys()):
                if (key in line):
                    match = re.search(pattern, line)

                    call_name = match.group(1)
                    parameters = match.group(2).split(',')

                    return_type = method_dict[call_name][0]
                    for i in range(len(parameters)):
                        mutated_params = []
                        for j in parameters:
                            mutated_params.append(j.strip())

                        param_type = method_dict[call_name][1][i][1]
                        new_param = substitute(mutated_params[i], param_type)
                        if (mutated_params[i] == new_param):
                            continue
                        mutated_params[i] = new_param
                        
                        new_line = line.replace(match.group(0), call_name + '(' + ', '.join(mutated_params) + ')', 1)

                        mutations.append([module, method, idx, new_line, line])

                    for i in range(len(parameters)):
                        mutated_params = []
                        for j in parameters:
                            mutated_params.append(j.strip())

                        param_type = method_dict[call_name][1][i][1]
                        if (param_type == 'int'):
                            new_param = f'-{mutated_params[i]}'
   
                            mutated_params[i] = new_param
                        
                            new_line = line.replace(match.group(0), call_name + '(' + ', '.join(mutated_params) + ')', 1)

                            mutations.append([module, method, idx, new_line, line])


                    match1 = re.search(pattern1, line)
                    if (match1):
                        if (return_type != None):
                            new_return_value =  substitute("", return_type)
                            new_line = line.replace(match.group(0), new_return_value)
                            mutations.append([module, method, idx, new_line, line])
                    else:
                        new_line = line.replace(match.group(0), "")
                        mutations.append([module, method, idx, new_line, line])

        else:
            method = ""


def copy_input_file(file):
    f = open(file)
    lines = f.readlines()
    module_code[file] = [line for line in lines if line.rstrip()]
                

def run_tests(method):
    import test_cases
    if hasattr(test_cases.IntegrationTests, method):
        suite = unittest.TestSuite()
        suite.addTest(test_cases.IntegrationTests(method))
        result = unittest.TextTestRunner().run(suite)
        return result.wasSuccessful()
    return -1




args_len = len(argv)
module_list = []
for i in range(1, args_len):
    module_list.append(argv[i])

for m in module_list:
    copy_input_file(m)
    get_function_names(m)


for m in module_list:
    mutate_function_calls(m)

for mutation in mutations:
    module, method, ln, change, _ = mutation
    print("Mutation : ", mutation)
    
    write_mutation(module, ln, change)
    mutant_result = run_tests("test_"+method)
    # print(mutant_result)
    # print("Rewriting the file")
    write_file_back(module)
    # print("Running tests on the original file.")
    og_result = run_tests("test_"+method)
    print("printing the 2 results" + str(mutant_result) + str(og_result))
    if (mutant_result != -1):

        mutants_created += 1
        if (og_result != mutant_result):
            mutants_killed += 1
    print("------------------------------------------------------------------------------------------------------------")

print("Number of mutants created : ", mutants_created)
print("Number of mutants killed : ", mutants_killed)
print("MUTATION SCORE : " , mutants_killed/mutants_created)

