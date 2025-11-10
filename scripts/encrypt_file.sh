#!/bin/bash

# use samples
# ./encrypt_file.sh 689bc3e3bdf1c5f2cff81725011ba7d3c0089b25 models/ configs/models

CRYPTOR_EXEC="/home/sinter/workspace/sk_breast/install/tools/cryptor"

if [ "$#" -ne 3 ]; then
    echo "Error: Invalid number of arguments."
    echo "Usage: $0 <commit> <input_dir> <output_dir>"
    echo "Sample: $0 689bc3e3bdf1c5f2cff81725011ba7d3c0089b25 ./input_models ./encrypted_models"
    exit 1
fi

COMMIT="$1"
INPUT_DIR="$2"
OUTPUT_DIR="$3"

if [ ! -x "$CRYPTOR_EXEC" ]; then
    echo "Error: Encryptor '$CRYPTOR_EXEC' not found or not executable." >&2
    echo "Please ensure '$CRYPTOR_EXEC' is in the current directory and is executable." >&2
    exit 1
fi

if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory '$INPUT_DIR' not found or not a directory." >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
if [ $? -ne 0 ]; then
    echo "Error: Could not create output directory '$OUTPUT_DIR'." >&2
    exit 1
fi

echo "Processing directory: '$INPUT_DIR'"
echo "Commit ID: $COMMIT"
echo "Outputting to directory: '$OUTPUT_DIR'"
echo "-------------------------------------"

find "$INPUT_DIR" -maxdepth 1 -type f | while IFS= read -r input_file; do
    filename=$(basename "$input_file")

    if [[ "$filename" =~ ^([^.]+)(\..*)?$ ]]; then
        first_part="${BASH_REMATCH[1]}"
        rest_part="${BASH_REMATCH[2]}"

        if [[ -z "$rest_part" ]]; then
            output_filename="${first_part}.enc"
        elif [[ -z "$first_part" ]]; then
            output_filename="${filename}.enc"
        else
            output_filename="${first_part}.enc${rest_part}"
        fi
    else
        echo "Warning: Filename '$filename' has an unusual format, appending .enc to the end." >&2
        output_filename="${filename}.enc"
    fi

    output_file="$OUTPUT_DIR/$output_filename"

    echo "Processing: '$filename' -> '$output_filename'"

    "$CRYPTOR_EXEC" --mode "encrypt" --input "$input_file" --output "$output_file" --commit "$COMMIT"

    if [ $? -ne 0 ]; then
        echo "Error: Failed to encrypt file '$filename'." >&2
    fi
done

echo "-------------------------------------"
echo "Done"
exit 0
