def write_output(validation_type, status, details):
    # Write the output to the report file
    with open("/Users/admin/PycharmProjects/test_automation_framework_ws/report_folder/report.txt", "a") as report:
        report.write(f"{validation_type}: {status} Details: {details}\n\n")