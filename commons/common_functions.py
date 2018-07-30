import csv


# Selection Function
def get_selection(print_text):
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


# return Elapse time
def get_elapsed_time_msg(start_time, end_time):

    try:
        s_time = float(start_time)
        e_time = float(end_time)

        return "Elapsed Time: {:.2f} Sec.".format(max(s_time, e_time) - min(s_time, e_time))

    except ValueError as err:
        raise ValueError(err)


# return Commit Message
def get_commit_msg(commit_value):
    return "{} Commit is occurred".format(commit_value)
