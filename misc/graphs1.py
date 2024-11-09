#read line by line from results.txt
#parse the line and create a graph



import sys
import matplotlib.pyplot as plt
import numpy as np
import re


#open the file
f = open("results1.txt", "r")
#iterate through the file

#initialize the lists
suspect_dict = {}
non_suspect_dict = {}
suspect_run = False
for line in f:
    #split the line
    if "SUSPECT" in line:
        suspect_run = True
        continue
    line = line.split(":")
    
    if(suspect_run):
        key = line[0].replace(" ","")
        #add to the suspect dictionary
        value1 = float(line[1])
        total = 0 
        total = (value1)/1024
        print(total,suspect_run)
        #check if the suspect is already in the dictionary
        if(key in suspect_dict):
            #add the values
            suspect_dict[key] += total
        else:
            suspect_dict[key] = total
            #print(suspect_dict)
            
    else:
        #add to the non suspect dictionary
        value1 = float(line[1])
        total = 0 
        total = (value1)/1024
        print(total,suspect_run)
        #check if the suspect is already in the dictionary
        key = line[0].replace(" ","")
        if(line[0] in non_suspect_dict):
            #add the values
            
            non_suspect_dict[key] += total
        else:
            non_suspect_dict[key] = total
            #print(non_suspect_dict)

#convert the dictionaries to lists
suspect_list = []
non_suspect_list = []
for key in suspect_dict:
    suspect_list.append(suspect_dict[key])
    non_suspect_list.append(non_suspect_dict[key])

#print the lists

print(suspect_list)
print(non_suspect_list)

#now plot two bar graphs with standard deviation
suspect_mean = np.mean(suspect_list)
non_suspect_mean = np.mean(non_suspect_list)

suspect_std = np.std(suspect_list)
non_suspect_std = np.std(non_suspect_list)

# Data to plot
modes = ['PingAck', 'PingAck+S']
means = [ non_suspect_mean,suspect_mean]
std_devs = [non_suspect_std,suspect_std]

# Create bar graph with error bars
plt.figure(figsize=(8, 6))
plt.bar(modes, means, yerr=std_devs, capsize=5, color=['blue', 'red'], alpha=0.7)

# Add labels and title
plt.ylabel('KB/s')
plt.title('Bandwidth Usage of Suspect and Non-Suspect Modes')
plt.ylim([0, max(means) + max(std_devs) * 1.5])  # Adjust Y limit for better visibility

# Show the plot
plt.show()

