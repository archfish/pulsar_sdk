opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')

client = PulsarSdk::Client.create(opts)
lookup = PulsarSdk::Protocol::Lookup.new(client, 'pulsar://pulsar.reocar.lan')

lookup.lookup('persistent://rental_car/orders/created')
