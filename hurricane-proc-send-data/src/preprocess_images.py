from pathlib import Path
import pandas as pd
import base64

# Extract image paths from filesystem
path_strings = [
    "./data/original/images/train_another",
    "./data/original/images/validation_another"
]


def extract_image_attributes_metadata(path_strings):
    """[summary]

    Args:
        path_strings ([type]): [description]

    Returns:
        [type]: [description]
    """
    search_paths = [Path(path) for path in path_strings]

    folder_names = []
    filenames = []
    coordinates = []
    file_paths = []

    for paths in search_paths:
        for path in paths.rglob("*"):
            if path.parent.name not in [path.split("/")[-1] for path in path_strings]:
                folder_names.append(path.parent.name)
                filenames.append(path.name)
                coordinates.append(path.stem)
                file_paths.append(path)
            else:
                print(f"Path {path} should be excluded!")

    return folder_names, filenames, coordinates, file_paths


def assign_base64_column(path):
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read())


def list_encoded_images(file_paths):
    """[summary]

    Args:
        file_paths ([type]): [description]

    Returns:
        [type]: [description]
    """
    encoded_strings = []

    for path in file_paths:
        try:
            encoded_strings.append(assign_base64_column(path))
        except IsADirectoryError:
            encoded_strings.append("")
            print(f"Skipped {path}")

    return encoded_strings


def convert_to_df(folder_names, filenames, coordinates, file_paths, encoded_strings):
    """[summary]

    Args:
        folder_names ([type]): [description]
        filenames ([type]): [description]
        coordinates ([type]): [description]
        file_paths ([type]): [description]
        encoded_strings ([type]): [description]

    Returns:
        [type]: [description]
    """
    image_metadata = pd.DataFrame(
        {"damage_flag": folder_names,
         "filename": filenames,
         "coordinates": coordinates,
         "file_path": file_paths,
         "image_base64": encoded_strings
         }
    )

    return image_metadata


def add_coordinates(image_metadata):
    """[summary]

    Args:
        image_metadata ([type]): [description]

    Returns:
        [type]: [description]
    """
    image_metadata = image_metadata.assign(
        latitude=image_metadata["coordinates"].apply(
            lambda x: x.split("_")[1]),
        longitude=image_metadata["coordinates"].apply(
            lambda x: x.split("_")[0])
    )

    return image_metadata


# image_metadata[0:2].to_json(
#     "./data/preprocessed/stream/image_data_metadata.json", orient="records", default_handler=str)
if __name__ == "__main__":

    # Prepare data
    folder_names, filenames, coordinates, file_paths = extract_image_attributes_metadata(
        path_strings)

    encoded_strings = list_encoded_images(file_paths)

    image_metadata = convert_to_df(folder_names, filenames, coordinates,
                                   file_paths, encoded_strings)

    image_metadata = add_coordinates(image_metadata)

    # Select subset of columns and export to csv
    json_columns = ['damage_flag', 'image_base64', 'latitude', 'longitude']

    image_metadata = image_metadata.loc[:, json_columns]

    image_metadata.to_csv(
        "./data/preprocessed/images/images_metadata.csv",  index=False)
