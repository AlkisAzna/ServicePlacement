import json
import math

# Opening JSON file - Initial Response Times
with open('initial_response_times_with_stressing.json') as json_file:
    firstdict = json.load(json_file)

# Opening JSON file - Response Times after new Placement
with open('final_markov_with_bin_packing_with_stressing.json') as json_file:
    seconddict = json.load(json_file)

firstsum = 0
# Calculate Initial total repsone time sum
listofcouples = []
for x in sorted(firstdict.keys()):
    for y in sorted(firstdict[x].keys()):
        couples = []
        print(x + "->" + y)
        firstsum += int(firstdict[x][y])
        couples.append(int(firstdict[x][y]))
        listofcouples.append(couples)

counter = 0
secondsum = 0
# Calculate total repsone time sum after new placement
for x in sorted(seconddict.keys()):
    for y in sorted(seconddict[x].keys()):
        secondsum += int(seconddict[x][y])
        listofcouples[counter].append(int(seconddict[x][y]))
        counter += 1

print(listofcouples)
sqsum = 0
absdifsum = 0
for i in listofcouples:
    sqsum += math.pow(i[1] - i[0], 2)
    absdifsum += abs(i[1] - i[0])

variation = firstsum - secondsum
print("Variation after placement of the overall response time is: " + str(variation))

print("Mean Squared Error: " + str(sqsum / len(listofcouples)))

print("Sum of Absolute Differences:" + str(absdifsum))

if variation > 0:
    print("Reduced " + str(variation) + " ms")
elif variation < 0:
    print("Increased " + str(abs(variation)) + " ms")
else:
    print("No change was noticed!")
