import os

def repeat_file_contents(input_file, output_folder, repeat_count):
    os.makedirs(output_folder, exist_ok=True)

    with open(input_file, 'rb') as source_file:
        content = source_file.read()
        
        input_file_name = os.path.basename(input_file)
        
        for i in range(1, repeat_count + 1):
            output_file = os.path.join(output_folder, f"{i}x_{input_file_name}")
            with open(output_file, 'wb') as destination_file:
                for _ in range(i):
                    destination_file.write(content)
            print(f"Created file: {output_file}")


input_file = '../Test1/20mb.txt'  
output_folder = '../HYDFS/business'  
repeat_count = 5

repeat_file_contents(input_file, output_folder, repeat_count)