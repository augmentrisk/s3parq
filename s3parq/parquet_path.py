from urllib import parse


class ParquetPath:
    """parsed representation of a parquet path
    s3_path: str the true, full path to the parquet file in s3
    parts_type_map: construction element to type path parts; keys are partitions, values are the conversion callable
    path_parts: dictionary of typed path parts
    """
    def __init__(self, s3_path: str, parts_type_map: dict):
        self.s3_path = s3_path
        self.parts_type_map = parts_type_map

        self.path_parts = self._parse_path(s3_path)

    def _parse_path(self, s3_path: str) -> dict:
        """parse the path into parts and types"""
        path_parts = {}
        # skip the collection (first) and the file (last) parts
        partitions = s3_path.split('/')[1:-1]
        partition_pairs = {k: v for pair in partitions for k, v in [parse.unquote(pair).split('=', 1)]}
        for part in partition_pairs:
            path_parts[part] = self.parts_type_map[part](partition_pairs[part])
        return path_parts