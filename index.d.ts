/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

/**
 * A high-performance, in-memory search engine exposed as a Node.js addon.
 *
 * The `MicroSearch` struct wraps the `LogDB`, providing a simplified interface for
 * creating, updating, and querying documents. This struct is designed to be
 * instantiated and used from JavaScript code.
 */
export declare class MicroSearch {
  /**
   * Creates a new instance of `MicroSearch`.
   *
   * This constructor initializes a new `LogDB` with default settings and wraps it
   * in a `MicroSearch` struct, making it available for use in a Node.js environment.
   *
   * # Returns
   * A `Result` containing the new `MicroSearch` instance or an error if initialization fails.
   */
  constructor()
  /**
   * Inserts or updates a simple document with the given content.
   *
   * This method provides a straightforward way to add content to the search index
   * without specifying additional metadata like log level or service.
   *
   * # Arguments
   * * `content` - The string content of the document to be indexed.
   *
   * # Returns
   * A `Result` containing the document ID as a string, or an error if the operation fails.
   */
  upsertSimple(content: string): string
  /**
   * Inserts or updates a log entry with additional metadata.
   *
   * This method allows for the indexing of structured log data, including log level
   * and service name, which can be used for more advanced filtering and querying.
   *
   * # Arguments
   * * `content` - The main content of the log entry.
   * * `level` - An optional string specifying the log level (e.g., "INFO", "ERROR").
   * * `service` - An optional string specifying the service name.
   *
   * # Returns
   * A `Result` containing the document ID as a string, or an error if the operation fails.
   */
  upsertLog(content: string, level?: string | undefined | null, service?: string | undefined | null): string
  /**
   * Executes a search query and returns a list of matching document IDs.
   *
   * # Arguments
   * * `query` - The search query string.
   *
   * # Returns
   * A `Result` containing a vector of document IDs as strings, or an error if the query fails.
   */
  query(query: string): Array<string>
  /**
   * Executes a search query and returns the full content of matching documents.
   *
   * # Arguments
   * * `query` - The search query string.
   *
   * # Returns
   * A `Result` containing a vector of document content strings, or an error if the query fails.
   */
  queryContent(query: string): Array<string>
}
