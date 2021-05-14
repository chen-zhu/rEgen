import requests
import json

# API Doc: http://localhost:8080/kie-server/docs/#/
# Process ID is hard to find. Call endpoint to find out:
# http://localhost:8080/kie-server/services/rest/server/containers/$$container_id$$/processes
container_id = "Integration_Quote_Integration_V1/"
process_id = "Integration_Quote.Integration_Quote/"

general_url = "http://localhost:8080/kie-server/services/rest/server/containers/" + container_id
sample_bearer_token = "d2JhZG1pbjp3YmFkbWlu"
header = {
    'Content-Type': "application/json",
    'Authorization': "Basic " + sample_bearer_token
}


def createProcessInstance(json_payload):
    url = general_url + "processes/" + process_id + "instances"
    response = requests.request("POST",
                                url,
                                data=json_payload,
                                headers=header)
    #print(response, url)
    if response.status_code not in [200, 201]:
        raise Exception("createProcessInstance abnormal status_code " + str(response.status_code))
    return response.text


def retrieveActiveTasks(process_instance_id):
    response = requests.request("GET",
                                general_url + "processes/instances/" + process_instance_id,
                                headers=header)
    # print('retrieveActiveTasks', response.status_code)
    if response.status_code not in [200, 201]:
        raise Exception("retrieveActiveTasks abnormal status_code " + str(response.status_code))
    if response.json().get('active-user-tasks') is not None:
        return response.json().get('active-user-tasks').get('task-summary')
    else:
        return response.json().get('active-user-tasks')


def startActiveTask(task_id):
    url = general_url + "tasks/" + str(task_id) + "/states/started"
    payload = ""
    response = requests.request("PUT", url, data=payload, headers=header)

    # print('startActiveTask', response.status_code)
    if response.status_code not in [200, 201]:
        raise Exception("startActiveTask abnormal status_code " + str(response.status_code))


def completeActiveTask(task_id, json_payload):
    url = general_url + "tasks/" + str(task_id) + "/states/completed"
    response = requests.request("PUT", url, data=json_payload, headers=header)

    # print('completeActiveTask', response.status_code)
    if response.status_code not in [200, 201]:
        raise Exception("completeActiveTask abnormal status_code " + str(response.status_code))


def claimActiveTask(task_id):
    url = general_url + "tasks/" + str(task_id) + "/states/claimed"
    response = requests.request("PUT", url, data="", headers=header)

    # print('claimActiveTask', response.status_code)
    if response.status_code not in [200, 201]:
        raise Exception("claimActiveTask abnormal status_code " + str(response.status_code))
