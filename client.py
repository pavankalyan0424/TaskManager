import socket
import json
import os

SCHEDULER_HOST = '127.0.0.1'
SCHEDULER_PORT = 50000


def make_rpc_call(task, file_path):
    try:
        with open(file_path, 'r') as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)

        request = {
            'task': task,
            'file_name': file_name,
            'file_data': file_data
        }
        message = json.dumps(request).encode('utf-8')

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((SCHEDULER_HOST, SCHEDULER_PORT))
            s.sendall(message)
            response_data = s.recv(4096)

        # Check if the response is valid JSON
        try:
            response = json.loads(response_data.decode('utf-8'))
            return response
        except json.JSONDecodeError:
            return {'status': 'error', 'message': 'Invalid response from scheduler.'}

    except Exception as e:
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    file_to_process = input("Enter the path to the file you want to process: ")

    if not os.path.exists(file_to_process):
        print("Error: File not found.")
    else:
        print(f"\nSubmitting 'word_count' task...")
        result = make_rpc_call('word_count', file_to_process)

        # Add robust error handling
        if 'status' in result and result['status'] == 'success':
            print(f"Word count result: {result['result']}")
        elif 'message' in result:
            print(f"Error: {result['message']}")
        else:
            print("An unexpected error occurred.")