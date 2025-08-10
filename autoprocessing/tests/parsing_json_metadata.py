import os
import json
import glob

def find_and_parse_metadata(base_path):
    # Recursive search for files like beamtime-metadata*.json
    pattern = os.path.join(base_path, "**", "beamtime-metadata*.json")
    json_files = glob.glob(pattern, recursive=True)

    for json_file in json_files:
        try:
            with open(json_file, "r") as f:
                data = json.load(f)

            # Ensure required keys exist
            if all(k in data for k in ["beamtimeId", "onlineAnalysis"]):
                online = data["onlineAnalysis"]
                result = {
                    "beamtimeId": data["beamtimeId"],
                    "reservedNodes": online.get("reservedNodes", []),
                    "sshPrivateKeyPath": online.get("sshPrivateKeyPath"),
                    "sshPublicKeyPath": online.get("sshPublicKeyPath"),
                    "userAccount": online.get("userAccount"),
                    "file": json_file  # Optional: log source
                }
                return result  # First valid match

        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Skipping {json_file}: {e}")
            continue

    raise FileNotFoundError("No valid beamtime-metadata*.json file found with required fields.")

# Example usage
if __name__ == "__main__":
    print("Beamtime Metadata Found:")
    base_path = os.path.dirname(os.path.abspath(__file__))
    print(f"Searching in base path: {base_path}")
    metadata = find_and_parse_metadata(base_path)
    for k, v in metadata.items():
        print(f"{k}: {v}")
