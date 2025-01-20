import processor_1
import processor_2
import processor
import argparse
import mailer_1


def runner():

    folder_path_1, order_df = processor_1.main()
    folder_path_2, quotes_df = processor_2.main()

    print(f"folder path 1 {folder_path_1}")
    print(f"folder path 2 {folder_path_2}")

    mailer_1.sender(folder_path_1, folder_path_2, order_df, quotes_df)



if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(description="script to run")

    runner()