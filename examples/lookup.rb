opts = PulsarSdk::Options::Client.new(url: 'pulsar://pulsar.reocar.lan')

client = PulsarSdk::Client.new(opts)
lookup = PulsarSdk::Protocol::Lookup.new(client, 'pulsar://pulsar.reocar.lan')

lookup.lookup('persistent://rental_car/orders/created')
