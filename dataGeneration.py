import csv
from faker import Faker
import datetime

fake = Faker()
current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
RECORD_COUNT = 10  # For example

def create_csv_file():
    with open(f'FakeDataset/customer_{current_time}.csv', 'w', newline='') as csvfile:
        fieldnames = ["customer_id","first_name","last_name","email","street",
                      "city","state","country"
                     ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(RECORD_COUNT):
            writer.writerow(
                {
                    "customer_id": i,
                    'first_name': fake.first_name(),
                    'last_name': fake.last_name(),
                    'email': fake.email(),
                    'street': fake.street_address(),
                    'city': fake.city(),
                    'state': fake.state(),
                    'country': fake.country()
                }
            )

# Call the function to create the CSV file
create_csv_file()