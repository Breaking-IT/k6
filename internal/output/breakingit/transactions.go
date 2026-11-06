package breakingit

import "fmt"

type transactionSource uint8

const (
	// Keep both existing mappings enabled by default for backwards compatibility.
	transactionsBoth transactionSource = iota
	transactionsPages
	transactionsGroups
)

func parseTransactionSource(value string) (transactionSource, error) {
	switch value {
	case "", "both":
		return transactionsBoth, nil
	case "pages":
		return transactionsPages, nil
	case "groups":
		return transactionsGroups, nil
	default:
		return transactionsBoth, fmt.Errorf(
			"invalid BREAKINGIT_TRANSACTION_SOURCE %q: expected both, pages, or groups", value)
	}
}
