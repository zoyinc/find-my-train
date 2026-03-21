newTrainSetHistory = '117,578,117,117,117,644,578,644'

#
# Define the rules for when a train is in front.
#
# It is a series of strings that look like '2/3'
# For example '2/3' means that 2 of the 3 most recent front trains as defined
# by newTrainSetHistory.
# newTrainSetHistory would look something like:
#
#  newTrainSetHistory = '578,578,644,578,644'
#
#  Order the rules in the sequence you want them evaluated
#  =======================================================
#
#  Once a rule has been satisified the rule evaluations stop.
#
frontTrainRules = ['2/2', '2/3','4/6']
newTrainSetHistoryList = newTrainSetHistory.split(',')

print('frontTrainRules: ', frontTrainRules)
print('newTrainSetHistory: ', newTrainSetHistory)
trainFoundInFront = None
for currRuleStr in frontTrainRules:
    minOccurrancesRequired, noToConsider = map(int, currRuleStr.split('/'))
    print(f'\n\ncurrRuleStr: {currRuleStr} , minOccurrancesRequired: {minOccurrancesRequired}, noToConsider: {noToConsider}')

    newTrainSetHistoryListTruncated = newTrainSetHistoryList[:noToConsider]
    maxOccurrancesFound = 0
    trainWithMaxOccurrances = None
    for trainInSet in newTrainSetHistoryListTruncated:
        print(f' - trainInSet: {trainInSet}')
        noOccurances = newTrainSetHistoryListTruncated.count(trainInSet)
        print(f' - noOccurances: {noOccurances}')
        if noOccurances > maxOccurrancesFound:
            maxOccurrancesFound = noOccurances
            trainWithMaxOccurrances = trainInSet
    print(f' - trainWithMaxOccurrances: {trainWithMaxOccurrances}, maxOccurrancesFound: {maxOccurrancesFound}')

    if maxOccurrancesFound >= minOccurrancesRequired:
        print(f' - Train {trainWithMaxOccurrances} is in front according to rule {currRuleStr}')    
        trainFoundInFront = trainWithMaxOccurrances
    else:
        print(f' - No train is in front according to rule {currRuleStr}')

    if trainFoundInFront is not None:
        break
print(f'\n\nFinal train found in front: {trainFoundInFront}')
