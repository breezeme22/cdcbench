import csv


# Selection Function
def selection_return(print_text):
    user_input = input(print_text)

    if len(user_input) == 0 or user_input is None:
        user_input = "N"

    user_input = user_input.strip().upper()

    if user_input == "Y":
        return True
    elif user_input == "N":
        return False
    else:
        return None


# Data file read
def get_data(file_name):

    try:
        f = open(file_name, "r", encoding="utf-8")
        reader = csv.reader(f)
        reader = list(reader)
        f.close()

        return reader

    except FileNotFoundError:
        raise FileNotFoundError("Data file ({}) does not exist.".format(file_name))
