import gradio as gr
import os
from gradio.themes.utils import sizes
from databricks.sdk import WorkspaceClient
import json
import time
from datetime import datetime 
import pandas as pd
import yaml
with open('app.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)


api_prefix = config['databricks']['api_prefix']
max_retries = config['databricks']['max_retries']
retry_delay = config['databricks']['retry_delay']
new_line = "\r\n"
databricks_host = config['databricks']['host'] 
master_config_warehouse_id = config['databricks']['master_config']['warehouse_id']
master_config_catalog = config['databricks']['master_config']['catalog']
master_config_schema = config['databricks']['master_config']['schema']
globals()["space_dict_list"] = []
globals()["previous_message"] = ''




def get_genie_space_id_statement(token, message):
    w = WorkspaceClient(host = databricks_host,token=token)
    statement = f"select  space_id,genie_space from bimal_demo.alex_test.master_config\
    where ai_query( 'databricks-meta-llama-3-1-405b-instruct', concat( ' The the explanation of the dataset in a database is in the section: explanation.  \
    if i were to ask this question: \"{message}\", will this dataset have the information i seek. respond \
    with nothing but True or False','<explanation>',data_set_explanation,'</explanation>',\
        '<previous_conv>{globals()['previous_message']}<previous_conv>'), schema_of_json('\"True\"')) = True"

    # Execute the statement
    try:
        result = w.statement_execution.execute_statement(
                catalog = master_config_catalog,
                schema = master_config_schema,
                statement=statement,
                warehouse_id=master_config_warehouse_id
            )
        while result.status.state.value == 'PENDING' or result.status.state.value == 'RUNNING':
            time.sleep(retry_delay)
            result = w.statement_execution.get_statement(
                result.statement_id
            )
        if result.result is not None:

            _space_dict_list = [{'space_id':x[0], 'space_name':x[1], 'conversation_id': None, 'isCurrent': True} for x in result.result.data_array]
            for idx, item in enumerate(globals()["space_dict_list"]):
                item['isCurrent'] = False
                globals()["space_dict_list"][idx] = item

            for d in _space_dict_list:
                if d["space_id"] in [x['space_id'] for x in globals()["space_dict_list"]]:
                    for idx, item in enumerate(globals()["space_dict_list"]):
                        if item['space_id'] == d["space_id"]:
                            item['isCurrent'] = True
                            globals()["space_dict_list"][idx] = item
                else:
                        globals()["space_dict_list"].append(d)    

    except Exception as g:
        print("calling workspace client failed:", str(g))

    # Process the result as needed
    print(" The output of the query is :", result)
    globals()["previous_message"] = message
    


def get_genie_response(w,conversation_id, message_id, space_id):
    attempt = 0
    has_sql = False 
    while attempt < max_retries:
        resp = w.api_client.do(
                method="GET",
                path=f"{api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                headers={"Content-Type": "application/json"},
            )
        status = resp["status"]
        if status == "EXECUTING_QUERY":
            attempt = 0
            while attempt < max_retries:
                
                resp_qs = w.api_client.do(
                    method="GET",
                    path=f"{api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result",
                    headers={"Content-Type": "application/json"},
                )
                status = resp_qs['statement_response']['status']
                
                if status['state'] == "SUCCEEDED":
                    resp = w.api_client.do(
                            method="GET",
                            path=f"{api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                            headers={"Content-Type": "application/json"},
                            )
                    try:
                        # result = f"""
                        # SQL: {resp["attachments"][0]["query"]["query"]} 
                        # {new_line}{new_line}
                        # Description: {resp["attachments"][0]["query"]["description"]}
                        # """
                        result = resp["attachments"][0]["query"]
                        return resp_qs,result
                    except:
                        # result = resp["attachments"][0]["text"]["content"]
                        result = {'query':'','description':resp["attachments"][0]["text"]["content"]}
                        return resp_qs,result
                    
                elif status != "COMPLETED" and attempt < max_retries - 1:
                    time.sleep(retry_delay)

                else:
                    return (
                        f"Query failed or still running after {max_retries*retry_delay} seconds"
                    )

                attempt += 1
        elif status == "COMPLETED":
                # result = resp["attachments"][0]["text"]["content"]
                result = {'query':'','description':resp["attachments"][0]["text"]["content"]}
                return resp,result
        elif status != "COMPLETED" and attempt < max_retries - 1:
                time.sleep(retry_delay)
    attempt += 1

    
    return resp,result
    
def genie_respond(message, history, request: gr.Request):
    token = request.headers.get("x-forwarded-access-token")
    
    # space_id = get_genie_space_id(token, message)
    get_genie_space_id_statement(token, message)
    results = ''
    _space_dict_list = globals()["space_dict_list"]
    for idx, item in enumerate(_space_dict_list): 
        if item['isCurrent'] == True:
            df = pd.DataFrame()
            w = WorkspaceClient(host = databricks_host,token=token)
            conversation_id = item['conversation_id']
            space_id = item['space_id']
            space_name = item['space_name']
            if not history or conversation_id is None:
                path = f"{api_prefix}/{space_id}/start-conversation"
            else:
                path = f"{api_prefix}/{space_id}/conversations/{conversation_id}/messages"
            resp = w.api_client.do(
                method="POST",
                path=path,
                headers={"Content-Type": "application/json"},
                body={"content": message},
            )
            conversation_id = resp["conversation_id"]
            try:
                message_id = resp["message_id"]
            except:
                message_id = resp["id"]

            item['conversation_id'] = conversation_id
            _space_dict_list[idx] = item
            resp,result = get_genie_response(w,conversation_id, message_id, space_id)
            if 'statement_response' in resp:
                columns = resp["statement_response"]["manifest"]["schema"]["columns"]
                header = [str(col["name"]) for col in columns]
                if 'data_typed_array' in resp["statement_response"]["result"]:
                    values = [x['values'] for x in resp["statement_response"]["result"]["data_typed_array"]]
                    row_values = []
                    for row in values:
                        row_values.append([x['str'] if 'str' in x else '' for x in row])
                    df = pd.DataFrame(row_values, columns = header)
                    results = results+ create_html(result, space_name, df)
                else:
                    results = results+ create_html(result, space_name, df)
            else: 
                results = results+ create_html(result, space_name,df)


    globals()["space_dict_list"] = _space_dict_list   
    return results
 
def create_html(result, space_name, df):
    head = ''
    # head = '<head><meta name="viewport" content="width=device-width, initial-scale=1"><style>.accordion {background-color: color: cursor: pointer;padding: 18px;width: 100%;border: none;text-align: left;outline: none;font-size: 15px;transition: 0.4s;}\
    # .active, .accordion:hover {background-color: #ccc;}.panel {padding: 0 18px;display: none;background-color: white;overflow: hidden;}</style></head>'
    script = ''
    # script = '<script>var acc = document.getElementsByClassName("accordion");var i;\
    #     for (i = 0; i < acc.length; i++) {\
    #     acc[i].addEventListener("click", function() {\
    #         this.classList.toggle("active");\
    #         var panel = this.nextElementSibling;\
    #         if (panel.style.display === "block") {\
    #         panel.style.display = "none";\
    #         } else {\
    #         panel.style.display = "block";\
    #         }\
    #     });}</script>'
    if df.shape[0]>0:
        body = f"<body><p>Genie Space: {space_name} </p> <p>{result['description']}</p>" + df.to_html().replace('\n','') 
        # body = body + f'<body><button class="accordion">See Query</button><div class="panel"><p>{result["query"]}</p></div>'
        body = body + f'<body><div class="panel"><p>Query: {result["query"]}</p></div>'
    else:
        body = f"<p>Genie Space: {space_name} :{result['description']}</p>"
    redered_html = head + body  + script + '</body>'
    return redered_html

theme = gr.themes.Base(
    primary_hue=gr.themes.colors.red,
    secondary_hue=gr.themes.colors.pink,
    text_size=sizes.text_sm,
    radius_size=sizes.radius_sm,
    spacing_size=sizes.spacing_sm,
)

demo = gr.ChatInterface(
    genie_respond,
    chatbot=gr.Chatbot(
        show_label=False, container=False, show_copy_button=True, bubble_full_width=True
    ),
    textbox=gr.Textbox(placeholder="Ask me a question", container=False, scale=7),
    title="Explore & Discover with Multiple - Genie spaces",
    description="This chatbot is a demo example on how to chat with Genie.",
    examples=[
        ["What tables are there and how are they connected? Give me a short summary?"],
        ["show me top 10 count of user interactions for each event name"],
        ["show me the top 10 order ids based on purchase revenue"],
        ["show me the top products by price"],
        ["how much will it cost me if i purchase 2 units of Premium King Mattress"],
    ],
    # additional_inputs=[space_id],
    cache_examples=False,
    theme=theme,
    retry_btn=None,
    undo_btn=None,
    clear_btn="Clear",
)


if __name__ == "__main__":
    demo.launch(share=True)
