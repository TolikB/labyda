# Live Wallet Order Path

This repo now has an evidence-backed runtime path for the current funded wallets on Polymarket and Myriad.

## Current wallet reality

- Polymarket signer EOA: `0x481622ac0c0f505d443F4CAAC1Ff09C7DFdD9E84`
- Polymarket funded SAFE wallet: `0x6f93865A536BcF6ef4B79e527de67ECdce0F989A`
- Myriad trader wallet: `0xEC75768CCfD5308814789A4835c9E96952908Fe0`

## What works

- Polymarket balance visibility and live orders work through SAFE mode:
  - `signature_type=2`
  - `funder=0x6f93865A536BcF6ef4B79e527de67ECdce0F989A`
- Myriad balance visibility and live orders work only with:
  - `collateral_symbol=USD1`

## One-command readiness report

Use this to confirm both venues before any live order:

```powershell
.\.venv312\Scripts\python.exe scripts\live_balance_and_order_readiness.py `
  --config config.json `
  --polymarket-condition-id 0x0e7b7cc2649466ce6dfed9cf49611630fe986b31fba84ec01107e0a50f1534bb `
  --polymarket-token-id 43187333641922996188398060383389814287787647811837308994701068387397271207198 `
  --polymarket-side BUY `
  --polymarket-price 0.03 `
  --polymarket-size 5 `
  --myriad-market-id 1335 `
  --myriad-outcome-side YES `
  --myriad-order-side BUY `
  --myriad-price 0.40 `
  --myriad-size 5
```

Expected live facts:

- Polymarket `visible_balance_usd` is non-zero through SAFE mode.
- Myriad `visible_balance_usd` is non-zero through `USD1`.
- Both order previews show the intended maker/trader, side, price, and size.

## Polymarket live path

Preview only:

```powershell
.\.venv312\Scripts\python.exe scripts\polymarket_safe_order_preview.py `
  --config config.json `
  --condition-id 0x0e7b7cc2649466ce6dfed9cf49611630fe986b31fba84ec01107e0a50f1534bb `
  --token-id 43187333641922996188398060383389814287787647811837308994701068387397271207198 `
  --side BUY `
  --price 0.03 `
  --size 5
```

Guarded live submit:

```powershell
$env:POLYMARKET_ORDER_SUBMIT_CONFIRM='YES'
.\.venv312\Scripts\python.exe scripts\polymarket_safe_order_preview.py `
  --config config.json `
  --condition-id 0x0e7b7cc2649466ce6dfed9cf49611630fe986b31fba84ec01107e0a50f1534bb `
  --token-id 43187333641922996188398060383389814287787647811837308994701068387397271207198 `
  --side BUY `
  --price 0.03 `
  --size 5 `
  --confirm-post-order
```

## Myriad live path

Preview only:

```powershell
.\.venv312\Scripts\python.exe scripts\myriad_balance_and_order_preview.py `
  --config config.json `
  --market-id 1335 `
  --side YES `
  --order-side BUY `
  --price 0.40 `
  --size 5
```

Guarded live submit:

```powershell
$env:MYRIAD_ORDER_SUBMIT_CONFIRM='YES'
.\.venv312\Scripts\python.exe scripts\myriad_balance_and_order_preview.py `
  --config config.json `
  --market-id 1335 `
  --side YES `
  --order-side BUY `
  --price 0.40 `
  --size 5 `
  --confirm-place-order
```

## Deposit-wallet path

The canonical Polymarket deposit wallet path is separate and is not required for immediate live trading from the currently funded SAFE wallet.

- Canonical deposit wallet: `0x0933450112c3911f1c7120e632A17E3fe530C79C`
- Current state: undeployed for `signature_type=3`
- Use the deposit-wallet helper scripts only if you intentionally want to migrate from SAFE mode into `POLY_1271`.
