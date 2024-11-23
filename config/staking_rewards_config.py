API_ENDPOINT = 'https://api.stakingrewards.com/public/query'
QUERY_LIMIT = 500

BLOCKCHAINS = {
    'ethereum-2-0': 'ethereum',
    'bittensor': 'bittensor',
    'solana': 'solana',
    'near-protocol': 'near',
    'aptos': 'aptos',
    'avalanche': 'avalanche',
    'the-open-network': 'ton',
    'celestia': 'celestia',
    'sui': 'sui',
    'binance-smart-chain': 'binance',
    'sei-network': 'sei'
}

METRICS = [
    'reward_rate',
    'real_reward_rate',
    'inflation_rate',
    'price',
    'staked_tokens',
    'staking_ratio'
]