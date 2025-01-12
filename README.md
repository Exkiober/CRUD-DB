python3 -m venv venv
source venv/bin/activate
pip freeze > requirements.txt
pip install -r requirements.txt



First, make sure there is database and table the database you connect
Run the read_from_db.py to generate a json format schema under directory: schema/
Save a copy for it first under directory: schema/
You now have two copies of schema
Select one to edit (Add the new column with dtype for either table or Add a new table)
Run the alter.py after locate the name and location of the schema files
It will generate the sql you need to change from old schema to latest schema you editted 
Insert it to your database and the structure/schema of your db will be changed
