import "node:worker_threads"

declare module "node:worker_threads" {
  type TransferListItem = Transferable
}
