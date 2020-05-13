/* Package writenotifier contains a simple subscription model usedby the top layers
of the application to know when a write was applied in the local storage.

For example the router will propose a change but it has to know know when the
write was applied in order to return the result to the API or retry if a timeout occurs.

Usually the life of a subscription is short (in time as a timeout and the scope)
ex: waits for for a specific Key for a few seconds.
*/

package writenotifier

import (
	"context"
	"sync"
)

// Subscription is basically a watcher submission made by various components.
type Subscription interface {
	// Match is called by the Dispatcher with all the written Keys
	Match(key []byte) bool
	// Valid is checked before each Match to see if the subscription is still active
	// At the first false result the Dispatcher will remove it from its list of subscriptions
	Valid() bool
}

// Dispatcher is the main coordinator that matches all the subscriptions with all the writes
type Dispatcher interface {
	AddSubscription(subscription Subscription)
	// Trigger will Match all the valid subscriptions and clean the ones that are not
	Trigger(batch [][]byte)
}

// SimpleDispatcher is a thread safe dispatcher
type SimpleDispatcher struct {
	m    sync.Mutex
	subs []Subscription
}

func (s SimpleDispatcher) AddSubscription(subscription Subscription) {
	panic("implement me")
}

func (s SimpleDispatcher) Trigger(batch [][]byte) {
	panic("implement me")
}

// SubscriptionByID is designed to watch for an unique key until a context is canceled/expired or a match is found
type SubscriptionByID struct {
}

func NewSubscriptionByID(ctx context.Context, key []byte) *SubscriptionByID {
	return nil
}

func (s SubscriptionByID) Match(key []byte) bool {
	panic("implement me")
}

func (s SubscriptionByID) Valid() bool {
	panic("implement me")
}
