from random import random
from random import seed


#
# CREATE TABLE ontario_cases (
# Row_ID VARCHAR,  Accurate_Episode_Date VARCHAR,  Case_Reported_Date VARCHAR,  Test_Reported_Date VARCHAR,  Specimen_Date VARCHAR, Age_Group VARCHAR, Client_Gender VARCHAR, Case_AcquisitionInfo VARCHAR, Outcome1 VARCHAR, Outbreak_Related VARCHAR, Reporting_PHU_ID VARCHAR, Reporting_PHU VARCHAR, Reporting_PHU_Address VARCHAR, Reporting_PHU_City VARCHAR, Reporting_PHU_Postal_Code VARCHAR, Reporting_PHU_Website VARCHAR, Reporting_PHU_Latitude VARCHAR, Reporting_PHU_Longitude VARCHAR
# )

# get names
def read_names():
    first_names = []
    last_names = []
    with open('first_name.csv') as first_name_file:
        lines = first_name_file.readlines()
        first_names = [line.strip() for line in lines]
    with open('last_name.csv') as last_name_file:
        lines = last_name_file.readlines()
        last_names = [line.strip() for line in lines]
    return first_names, last_names


def process_positive_seed():
    first_names, last_names = read_names()
    age_groups_map = {"<20": 0, "20s": 1, "30s": 2, "40s": 3, "50s": 4, "60s": 5, "70s": 6, "80s": 7, "90+": 8}

    seed(1)
    output = open('person_generated.csv', 'w')
    with open('person_seed.csv') as person_seed:
        lines = person_seed.readlines()
        id = 0
        output.write("id|first_name|last_name|gender|age_group|status|residency|variant|episode_date|report_date\n")
        for line in lines:
            episode_date = line.split('|')[1].strip()
            report_date = line.split('|')[2].strip()
            age_group = line.split('|')[3].strip()
            age_group_id = age_groups_map[age_group]
            gender = line.split('|')[4].strip()
            rand = random()
            variant_id = 0
            if rand < 0.4:
                variant_id = 0
            elif rand < 0.6:
                variant_id = 1
            elif rand < 0.9:
                variant_id = 3
            else:
                variant_id = 2
            output.write(str(id) + "|" + first_names[id] + "|" + last_names[id] + "|" + gender + "|" + str(
                age_group_id) + "|positive|" + str(id % 10) + "|" + str(
                variant_id) + "|" + episode_date + "|" + report_date)
            id = id + 1
            output.write("\n")
    output.close()
    return id


def process_negative_cases(id):
    first_names, last_names = read_names()
    age_groups_map = {"<20": 0, "20s": 1, "30s": 2, "40s": 3, "50s": 4, "60s": 5, "70s": 6, "80s": 7, "90+": 8}
    seed(100)
    output = open('person_generated.csv', 'a')
    for i in range(1, 40):
        if i % 2 == 0:
            gender = "MALE"
        else:
            gender = "FEMALE"
        rand = random()
        age_group = 1
        if rand <= 0.2:
            age_group = 0
        elif rand <= 0.3:
            age_group = 1
        elif rand <= 0.4:
            age_group = 2
        elif rand <= 0.5:
            age_group = 3
        elif rand <= 0.6:
            age_group = 4
        elif rand <= 0.7:
            age_group = 5
        elif rand <= 0.8:
            age_group = 6
        elif rand <= 0.9:
            age_group = 7
        else:
            age_group = 8
        output.write(str(id + i) + "|" + first_names[id] + "|" + last_names[id] + "|" + gender + "|" + str(
            age_group) + "|unknown|" + str(id % i) + "|" + "|" + "|\n")
        id = id + 1
    output.write("\n")


def main():
    id = process_positive_seed()
    process_negative_cases(id)


if __name__ == '__main__':
    main()
