from pg_database import Database


class Action:


    def __init__(self, table, column, type):
        # Database instance
        self.db = Database()

        # Action attributes
        self.table = table
        self.column = column
        self.type = type


    def __repr__(self):
        return str(self.table) + ',' + str(self.column) + ',' + str(self.type)


    def __hash__(self):
        return hash(str(self))


    def __eq__(self, other):
        return str(self) == str(other)


    def execute(self):
        # Execute the action
        if self.type == 'DROP':
            print(f"Dropped index on {self.column} in {self.table}")
            # Drop index
            self.db.drop_index(self.column, self.table)
        elif self.type == 'CREATE':
            print(f"Created index on {self.column} in {self.table}")
            # Create index
            self.db.create_index(self.column, self.table)
        else:
            print("PASS action!")



if __name__ == "__main__":
    action1 = Action("lineitem", "l_shipmode", "CREATE")
    action2 = Action("lineitem", "l_shipmode", "DROP")
    action1.execute()
    action2.execute()