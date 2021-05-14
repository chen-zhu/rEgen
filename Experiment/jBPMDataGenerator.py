from jBPM_REST import createProcessInstance, retrieveActiveTasks, startActiveTask, completeActiveTask, claimActiveTask
import json
from time import sleep
import random
import string


def random_num():
    return random.randint(1, 101)


def random_bool():
    P = 0.95  # probability of True
    if random_num() < P * 100:
        return True
    else:
        return False


def random_request_type():
    P = 0.9  # probability of "new"
    if random_num() < P * 100:
        return "new"
    else:
        return "old"


def random_API_type():
    P1 = 0.5  # probability of REST
    P2 = 0.3  # probability of SOAP
    num = random_num()
    if num < P1 * 100:
        return "REST"
    elif P1 * 100 <= num < (P1 + P2) * 100:
        return "SOAP"
    else:
        return "FTP"


def random_string(length=10):
    res = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return res


def payload_simulator(task_name):
    if task_name == 'Check Unpaid Bill':
        payload = {'hasUnpaidQuote': not random_bool()}
    elif task_name == 'Review Request':
        payload = {'requestType': random_request_type()}
    elif task_name == 'PM Evaluation':
        payload = {'comment': random_string(20), 'apiType': random_API_type()}
    elif task_name in ['REST API Team Inspect', 'SOAP API Team Inspect', 'FTP Developer Inspect', 'Developer Inspect']:
        payload = {'quoteTime': random.randint(1, 61)}
    elif task_name == 'PM Final Comment':
        payload = {'comment': random_string(20)}
    else:
        return {}
    return payload


def process_task_list(task_list, human_task_counter):
    random.shuffle(task_list)
    for task in task_list:
        task_id = task['task-id']
        if len(str(task_id)) == 0:
            continue
        task_status = task['task-status']
        if task_status == "Reserved":
            startActiveTask(task_id)
        elif task_status == "InProgress":
            submit_payload = payload_simulator(task['task-name'])
            sleep(1)
            completeActiveTask(task_id, json.dumps(submit_payload))
            human_task_counter[0] += 1
            if (human_task_counter[0] % 1000) == 0:
                print("\n*" + str(human_task_counter[0]) + "*")
        elif task_status == "Ready":
            claimActiveTask(task_id)


if __name__ == "__main__":
    #number_of_instances = 1
    number_of_human_tasks = 10 #24000 - 20000
    success_count = 0
    #for x in range(number_of_instances):
    x = [0]
    while x[0] < number_of_human_tasks:
        print(".", end="", flush=True)
        create_instance_payload = {
            "apiName": random_string(),
            "userName": random_string()
        }
        try:
            process_instance_id = createProcessInstance(json.dumps(create_instance_payload))

            while True:
                task_list = retrieveActiveTasks(process_instance_id)
                sleep(round(random.random()/5, 3))
                if task_list is None:
                    break
                random.shuffle(task_list)
                process_task_list(task_list, x)
                if x[0] >= number_of_human_tasks:
                    break
        except Exception as ex:
            process_instance_id = str(process_instance_id) if not None else ""
            print("Instance generation failed: process_instance_id: " + process_instance_id)
            continue
        success_count += 1

    print("\nGenerated " + str(success_count) + " instances within jBPM. Good Job!")
