import processor_1
import processor_2
import processor
import cgna_processor
import argparse
import mailer_1


def runner(scripts):

    for script in scripts: 

        if script == "quotes_orders":
            # orders and quotes
            folder_path_1, order_df = processor_1.main()
            folder_path_2, quotes_df = processor_2.main()

            mailer_1.sender(folder_path_1, folder_path_2, order_df, quotes_df)
        
            print(f"folder path 1 {folder_path_1}")
            print(f"folder path 2 {folder_path_2}")

        if script == "transfers_RMA_inventory_returns":
            # RMA and transfers
            processor.main()
            
        
        if script == "cgna":
            cgna_processor.main()

            


if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(description="script to run")
    parser.add_argument("--scripts_to_run", help="give the scripts to run as a list", required= True)
    args = parser.parse_args()

    scripts_raw = args.scripts_to_run
    scripts = scripts_raw.split(", ")

    runner(scripts)