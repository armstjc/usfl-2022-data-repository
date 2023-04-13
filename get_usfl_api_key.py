import json
import os
import platform

def get_usfl_api_key(key_path=""):
    try:
        key = os.environ['USFL_API_TOKEN']
        return key
    except:
        print('The USFL API token was not found in this python environment. Attempting to load the API token from a file.')

    print(platform.uname().system)
    try:
        if len(key_path) > 0:
            with open(key_path,'r') as f:
                json_data = json.loads(f.read())
        elif  platform.uname().system == "Windows":
            with open('C:/USFL/USFL_api.json','r') as f:
                json_data = json.loads(f.read())
        elif platform.uname().system == "Darwin":
            with open('/Users/Shared/USFL/USFL_api.json','r') as f:
                json_data = json.loads(f.read())
        else:
            raise FileNotFoundError('The USFL API token was not found in your computer.')    

        return json_data['usfl_api_token']
    except:
        raise FileNotFoundError('The USFL API token was not found in your computer.')    


if __name__ == "__main__":
    get_usfl_api_key()