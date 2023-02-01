from src.client import parse_nmon_csv, produce_outputs


def main():
    stream = produce_outputs(["sh", "scripts/nmon-to-stdout.sh"])
    parse_nmon_csv(stream)
    print("done")


if __name__ == "__main__":
    main()
