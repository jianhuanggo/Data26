import datetime

def set_default(list_item, check_val, set_val):

    new_list = []
    for index, val in enumerate(list_item):
        #print(f"HERE2!!!!! {str(val)} {str(set_val)}")
        if str(val) == str(check_val):
            new_list.append(set_val)
        else:
            new_list.append(val)

    return new_list


if __name__ == '__main__':
    print(datetime.datetime.min)
    test_list = ["not found", "not found", "2010"]
    a_list = set_default(test_list, "not found", datetime.datetime.min)

    for item in a_list:
        print(item)


