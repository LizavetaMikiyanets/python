import sys
import json
from typing import Dict, List
from argparse import ArgumentParser, FileType, ArgumentTypeError
from io import TextIOWrapper


DEFAULT_PATH_TO_STORE_INVERTED_INDEX = "inverted.index"


class EncodedFileType(FileType):
    """File encoder"""
    def __call__(self, string):
        # the special argument "-" means sys.std{in,out}
        if string == '-':
            if 'r' in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, encoding=self._encoding)
                return stdin
            if 'w' in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, encoding=self._encoding)
                return stdout
            msg = 'argument "-" with mode %r' % self._mode
            raise ValueError(msg)

        # all other arguments are used as file names
        try:
            return open(string, self._mode, self._bufsize, self._encoding,
                        self._errors)
        except OSError as exception:
            args = {'filename': string, 'error': exception}
            message = "can't open '%(filename)s': %(error)s"
            raise ArgumentTypeError(message % args)

    def print_encoder(self):
        """printer of encoder"""
        print(self._encoding)


class InvertedIndex:
    """
    This module is necessary to extract inverted indexes from documents.
    """
    def __init__(self, words_ids: Dict[str, List[int]]):
        self.words_ids = words_ids

    def query(self, words: List[str]) -> List[int]:
        """Return the list of relevant documents for the given query"""
        doc_indexes = []
        for word in words:
            if word in self.words_ids:
                doc_indexes.extend(self.words_ids[word])
        return doc_indexes

    def dump(self, filepath: str) -> None:
        """
        Allow us to write inverted indexes documents to temporary directory or local storage
        :param filepath: path to file with documents
        :return: None
        """
        with open(filepath, 'w') as f:
            json.dump(self.words_ids, f)

    @classmethod
    def load(cls, filepath: str):
        """
        Allow us to upload inverted indexes from either temporary directory or local storage
        :param filepath: path to file with documents
        :return: InvertedIndex
        """
        with open(filepath, 'r') as f:
            words_ids = json.load(f)
        return cls(words_ids)


def load_documents(filepath: str) -> Dict[int, str]:
    """
    Allow us to upload documents from either temporary directory or local storage
    :param filepath: path to file with documents
    :return: Dict[int, str]
    """
    documents = {}
    with open(filepath, 'r') as f:
        for line in f:
            doc_id, text = line.strip().split('\t')
            documents[int(doc_id)] = text
    return documents


def build_inverted_index(documents: Dict[int, str]) -> InvertedIndex:
    """
    Builder of inverted indexes based on documents
    :param documents: dict with documents
    :return: InvertedIndex class
    """
    words_ids = {}
    for doc_id, text in documents.items():
        cleaned_text = text.lower().split()
        for word in cleaned_text:
            if word not in words_ids:
                words_ids[word] = []
            words_ids[word].append(doc_id)
    return InvertedIndex(words_ids)


def callback_build(args):
    """
    Callback function for the "build" command.
    :param args: Parsed arguments
    """
    dataset_path = args.dataset
    output_path = args.output

    documents = load_documents(dataset_path)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(output_path)

def process_build(dataset, output) -> None:
    """
        Function is responsible for running of a pipeline to load documents,
        build and save inverted index.
        :param arguments: key/value pairs of arguments from 'build' subparser
        :return: None
        """
    documents: Dict[int, str] = load_documents(dataset)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(output)

def callback_query(args):
    """
    Callback function for the "query" command.
    :param args: Parsed arguments
    """
    query = args.query
    index_path = args.index

    inverted_index = InvertedIndex.load(index_path)
    relevant_docs = inverted_index.query(query)
    print(relevant_docs)

def process_query(queries, index) -> None:
    """
        Function is responsible for loading inverted indexes
        and printing document indexes for key words from arguments.query
        :param arguments: key/value pairs of arguments from 'query' subparser
        :return: None
    """
    inverted_index = InvertedIndex.load(index)
    for query in queries:
        print(query[0])
        if isinstance(query, str):
            query = query.strip().split()

        doc_indexes = ','.join(str(value) for value in inverted_index.query(query))
        print(doc_indexes)


def setup_subparsers(parser):
    """
    Initialize subparsers with arguments.
    :param parser: Instance of ArgumentParser
    """
    subparser = parser.add_subparsers(dest='command')

    build_parser = subparser.add_parser(
        "build",
        help="This parser is used to load, build, and save the inverted index based on documents."
    )
    build_parser.add_argument(
        '-d', '--dataset',
        required=True,
        help='Specify the path to the file with documents.'
    )
    build_parser.add_argument(
        '-o', '--output',
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help='Specify the path to save the inverted index. Default: %(default)s'
    )
    build_parser.set_defaults(callback=callback_build)

    query_parser = subparser.add_parser(
        "query",
        help="This parser is used to load and apply the inverted index."
    )
    query_parser.add_argument(
        '--index',
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help='Specify the path where the inverted index is located. Default: %(default)s'
    )
    query_file_group = query_parser.add_mutually_exclusive_group(required=True)
    query_file_group.add_argument(
        '-q', '--query', dest='query',
        action='append',
        nargs="+",
        help='Specify one or more queries to process.'
    )
    query_file_group.add_argument(
        '--query_from_file', dest='query',
        type=EncodedFileType("r", encoding='utf-8'),
        help="Specify a file containing queries for the inverted index."
    )
    query_parser.set_defaults(callback=callback_query)


def main():
    """
    Main function to parse arguments and execute corresponding commands.
    """
    parser = ArgumentParser(prog="Inverted Index")
    setup_subparsers(parser)
    arguments = parser.parse_args()

    if hasattr(arguments, 'callback'):
        arguments.callback(arguments)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
