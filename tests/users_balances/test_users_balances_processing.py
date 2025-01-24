import pytest
import numpy as np
import pandas as pd
from pandas import DataFrame


from ...src.users_balances.users_balances_processing_functions import (
    clean_atokens_vtokens_balances,
    clean_events,
    clean_liquidation_events,
    match_balances_with_events,
)


@pytest.fixture
def atokens():
    user_current_atoken_balance = [
        0.0,
        38522.637318,
        52522.611434,
        78113.96592,
        0.00131934,
        1.67214987,
        0.0,
        11.14015842,
        21.98624319,
        23.489249,
        18.49923861,
        15.37036062,
        1.23519273,
        76.235692,
    ]
    id = [
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x0d0e806aff90038bedba8b085cb1443bc6c2cb7fd016d72bee6293ac1c38558f",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x613b0e57e234a9772d98f05ac6115f8c5ab3a1a65ded992664858d11f91d191d",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe7531d4e6c3cdeaef4d76dd1d8c1e39c15fe98d96601cb65bd37ae7be91f9e11",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x79109534df8c257cbdfc9c93eaba2734f7d5fb10f5e458cec6d87e1f13da6d17",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0x79109534df8c257cbdfc9c93eaba2734f7d5fb10f5e458cec6d87e1f13da6d17",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xa56320b5668aa3bc87c9f868e0a96865b64857df72c4edac4125c1595a3f16a0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xd3eded331eb6c5b5dc182796c5ba24984cb6dc69e382b30d63d94a38e1c8a3ae",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xd892898714f879ee2be6c3d75ea75452aac59b5014e2a7465b3c12d31db8dd0a",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xf505daae995476278afa5b574da2df5365c35286716112ac5277b0fa615c7072",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe626ca8882664456b5e2540f6855a06374c747f361f10d7b4b07555f10a53b36",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40x2260fac5e5542a773aa44fbcfedf7c193bc2c5990x2f39d218133afab8f2b819b1066c7e434ad94e9e0xccaae9b955979e9c4c0123d1ba7f5fecf986d51dee61259ed12c8969e33c82f2",
        "0x7369e5c97db67c30f64ae2924728c952848713b60x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae90x2f39d218133afab8f2b819b1066c7e434ad94e9e0x91cb98262ca0e6aa74b8787de5e860dca1ed5a06f7f88f0c2c0cdae5ba7914cd",
    ]
    Index = [
        1.0378850587142248,
        1.0378845819193157,
        1.0378840704264691,
        1.0377692119777289,
        1.0017816555224357,
        1.0017789821940075,
        1.001778671538478,
        1.001778670498258,
        1.0017757101771163,
        1.001775701448935,
        1.0017751827160943,
        1.0017750364307556,
        1.0017731146321265,
        1.0,
    ]
    user_scaled_atoken_balance = [
        0.0,
        37116.494444,
        50605.470236,
        75271.038125,
        0.00131699,
        1.66918043,
        0.0,
        11.12037893,
        21.9472712,
        23.44761304,
        18.46645727,
        15.34312601,
        1.23300647,
        76.235692,
    ]
    timestamp = [
        1704774671,
        1704774503,
        1704774323,
        1704744455,
        1704744455,
        1704654371,
        1704629675,
        1704629579,
        1704340379,
        1704339479,
        1704309575,
        1704293435,
        1704136235,
        1704069563,
    ]
    pool = [
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    ]
    reserve_decimals = [6, 6, 6, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 18]
    reserve_name = [
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Wrapped BTC",
        "Aave Token",
    ]
    usage_as_collateral_enabled = [
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    user_address = [
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x7369e5c97db67c30f64ae2924728c952848713b6",
    ]

    return DataFrame(
        {
            "user_current_atoken_balance": user_current_atoken_balance,
            "id": id,
            "Index": Index,
            "user_scaled_atoken_balance": user_scaled_atoken_balance,
            "timestamp": timestamp,
            "pool": pool,
            "reserve_decimals": reserve_decimals,
            "reserve_name": reserve_name,
            "usage_as_collateral_enabled": usage_as_collateral_enabled,
            "user_address": user_address,
        }
    )


@pytest.fixture
def vtokens():
    user_current_variable_debt = [
        0.0,
        13902.526748,
        39483.417449,
        0.0,
        471589.3835,
        0.0,
        311824.759514,
        0.0,
        20078.735741,
        165007.808565,
        159698.69304,
        146676.7824275698,
        320006.771259,
        100000.0,
        113019.105213,
        100000.0,
        268000.0,
        0.0,
        0.0,
        14000.0,
        0.0,
        30037.629303,
        170000.0,
        60.0,
        0.0,
    ]
    id = [
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xd1127502fe3e73e5d59d6803145ce9e23220c2b74cc942e1e314004992a8dea4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x9ac8c3edcb8d1ebe47958163e652926b64377d637f62a1b5f6190dbcc28629dc",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xaf1af1a01afba952536e12e85357c73393f33ac7d7e11d44e07623c25dce2dff",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xd892898714f879ee2be6c3d75ea75452aac59b5014e2a7465b3c12d31db8dd0a",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x887d32314f79ff436db55afb78569d31d064b3680625ee59430327c512a6db6e",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb480x2f39d218133afab8f2b819b1066c7e434ad94e9e0x887d32314f79ff436db55afb78569d31d064b3680625ee59430327c512a6db6e",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x6b175474e89094c44da98b954eedeac495271d0f0x2f39d218133afab8f2b819b1066c7e434ad94e9e0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xc7ad823817921ff520f48c293b671eba4a85ebc4710d9cb4a863fb2377cc0fb8",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb480x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x6b175474e89094c44da98b954eedeac495271d0f0x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xf505daae995476278afa5b574da2df5365c35286716112ac5277b0fa615c7072",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb480x2f39d218133afab8f2b819b1066c7e434ad94e9e0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x6b175474e89094c44da98b954eedeac495271d0f0x2f39d218133afab8f2b819b1066c7e434ad94e9e0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb480x2f39d218133afab8f2b819b1066c7e434ad94e9e0xe626ca8882664456b5e2540f6855a06374c747f361f10d7b4b07555f10a53b36",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xd533a949740bb3306d119cc777fa900ba034cd520x2f39d218133afab8f2b819b1066c7e434ad94e9e0xd349830a9bbd67bb5f2481271a5e17b6d38c2d226c9551d3b0f082fb23b8cadf",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x1f9840a85d5af5bf1d1762f925bdaddc4201f9840x2f39d218133afab8f2b819b1066c7e434ad94e9e0x3551ae49921db3887314de4fe29f77d974e6e23831ba579e13e51b35af1fbdea",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x1f9840a85d5af5bf1d1762f925bdaddc4201f9840x2f39d218133afab8f2b819b1066c7e434ad94e9e0xdbb4f030cb8b98c253c4f0206cfd93da905e3fa84f14f0da1bc103c7b8814758",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x9f8f72aa9304c8b593d555f12ef6589cc3a579a20x2f39d218133afab8f2b819b1066c7e434ad94e9e0x6ffbf7e2ec2c7d46b9f517f5279f141d8f90c3d2346603e3adf3b3c378e58ad7",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb40xdac17f958d2ee523a2206206994597c13d831ec70x2f39d218133afab8f2b819b1066c7e434ad94e9e0x0135fda588f3f220e3eb367cb7c4506071c6a2704dfd9dddbd078673e81ff49b",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xd533a949740bb3306d119cc777fa900ba034cd520x2f39d218133afab8f2b819b1066c7e434ad94e9e0x0a4c48fd3a76e07478bf8fa7b9462c02122f9f421ce5ec6e72d5dbdfea534208",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00x9f8f72aa9304c8b593d555f12ef6589cc3a579a20x2f39d218133afab8f2b819b1066c7e434ad94e9e0x49a4e898cc8f5e17682075aae21a7231c98fa9a30827f6e33143f90248347dd0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f00xd533a949740bb3306d119cc777fa900ba034cd520x2f39d218133afab8f2b819b1066c7e434ad94e9e0xd8392fad0f2cadb51b75cc166bcde4f02818a6f68564fed09a007d0bb3be354a",
    ]
    Index = [
        1.051538606799878,
        1.0515379729056742,
        1.0510292927164284,
        1.050953834183093,
        1.0509528187890145,
        1.0459647695919383,
        1.050952699371801,
        1.0477456814445307,
        1.0507842840221913,
        1.050343236861044,
        1.0451711142392963,
        1.0471306361034685,
        1.0503398321717616,
        1.050268715750265,
        1.045045472678073,
        1.0470177421737754,
        1.0449709786736108,
        1.0859100286957226,
        1.009934418585609,
        1.0099143746033497,
        1.0130833815369262,
        1.0496274209045011,
        1.0855477068294046,
        1.0130469309077554,
        1.0855467756545034,
    ]
    user_scaled_variable_debt = [
        0.0,
        13221.136189,
        37566.42914,
        0.0,
        448725.551774,
        0.0,
        296706.749695,
        0.0,
        19108.332744,
        157098.939446,
        152796.696028,
        140074.96043987054,
        304669.747312,
        95213.728163,
        108147.547803,
        95509.36528772098,
        256466.452628,
        0.0,
        0.0,
        13862.561373580398,
        0.0,
        28617.420529,
        156602.9746371301,
        59.227265953252655,
        0.0,
    ]
    timestamp = [
        1704774587,
        1704774407,
        1704653963,
        1704629579,
        1704628967,
        1704628967,
        1704628895,
        1704628895,
        1704525659,
        1704340379,
        1704340379,
        1704340379,
        1704339479,
        1704309575,
        1704309575,
        1704309575,
        1704293435,
        1704291899,
        1704291899,
        1704168755,
        1704168671,
        1704136007,
        1704067907,
        1704067715,
        1704067271,
    ]
    pool = [
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    ]
    reserve_decimals = [
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        18,
        6,
        6,
        6,
        18,
        6,
        6,
        6,
        18,
        6,
        18,
        18,
        18,
        18,
        6,
        18,
        18,
        18,
    ]
    reserve_name = [
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "USD Coin",
        "Tether USD",
        "Dai Stablecoin",
        "Tether USD",
        "Tether USD",
        "USD Coin",
        "Dai Stablecoin",
        "Tether USD",
        "Tether USD",
        "USD Coin",
        "Dai Stablecoin",
        "USD Coin",
        "Curve DAO Token",
        "Uniswap",
        "Uniswap",
        "Maker",
        "Tether USD",
        "Curve DAO Token",
        "Maker",
        "Curve DAO Token",
    ]
    usage_as_collateral_enabled = [
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    user_address = [
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
    ]

    return DataFrame(
        {
            "user_current_variable_debt": user_current_variable_debt,
            "id": id,
            "Index": Index,
            "user_scaled_variable_debt": user_scaled_variable_debt,
            "timestamp": timestamp,
            "pool": pool,
            "reserve_decimals": reserve_decimals,
            "reserve_name": reserve_name,
            "usage_as_collateral_enabled": usage_as_collateral_enabled,
            "user_address": user_address,
        }
    )


@pytest.fixture
def balances(atokens, vtokens):
    return clean_atokens_vtokens_balances(vtokens)


@pytest.fixture
def events():
    action = [
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Borrow",
        "Supply",
        "Supply",
        "Supply",
        "Supply",
        "RedeemUnderlying",
        "RedeemUnderlying",
        "RedeemUnderlying",
        "RedeemUnderlying",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
        "Repay",
    ]
    amount = [
        19400.0,
        159924.353142,
        146867.926239,
        46666.0,
        46666.0,
        220000.0,
        100000.0,
        100000.0,
        268000.0,
        14000.0,
        7000.0,
        170000.0,
        60.0,
        78113.96592,
        0.43694991,
        0.16166129,
        76.235692,
        38522.655015,
        14000.0,
        25600.0,
        11.14015843,
        13902.535128,
        25600.0,
        471589.839133,
        159.764588,
        159819.960955,
        146.721205,
        146762.93487938782,
        9992.0,
        155000.0,
        155000.0,
        170056.74068204148,
        14000.27786093424,
        60.00215887110806,
        200033.39700409427,
    ]
    timestamp = [
        1704653963,
        1704628967,
        1704628895,
        1704340379,
        1704340379,
        1704339479,
        1704309575,
        1704309575,
        1704293435,
        1704168755,
        1704136007,
        1704067907,
        1704067715,
        1704744455,
        1704654371,
        1704136235,
        1704069563,
        1704774671,
        1704774503,
        1704774323,
        1704629675,
        1704774587,
        1704774407,
        1704629579,
        1704628967,
        1704628967,
        1704628895,
        1704628895,
        1704525659,
        1704340379,
        1704309575,
        1704291899,
        1704291899,
        1704168671,
        1704067271,
    ]
    txHash = [
        "0xaf1af1a01afba952536e12e85357c73393f33ac7d7e11d44e07623c25dce2dff",
        "0x887d32314f79ff436db55afb78569d31d064b3680625ee59430327c512a6db6e",
        "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
        "0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xf505daae995476278afa5b574da2df5365c35286716112ac5277b0fa615c7072",
        "0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xe626ca8882664456b5e2540f6855a06374c747f361f10d7b4b07555f10a53b36",
        "0xdbb4f030cb8b98c253c4f0206cfd93da905e3fa84f14f0da1bc103c7b8814758",
        "0x0135fda588f3f220e3eb367cb7c4506071c6a2704dfd9dddbd078673e81ff49b",
        "0x0a4c48fd3a76e07478bf8fa7b9462c02122f9f421ce5ec6e72d5dbdfea534208",
        "0x49a4e898cc8f5e17682075aae21a7231c98fa9a30827f6e33143f90248347dd0",
        "0x79109534df8c257cbdfc9c93eaba2734f7d5fb10f5e458cec6d87e1f13da6d17",
        "0xa56320b5668aa3bc87c9f868e0a96865b64857df72c4edac4125c1595a3f16a0",
        "0xccaae9b955979e9c4c0123d1ba7f5fecf986d51dee61259ed12c8969e33c82f2",
        "0x91cb98262ca0e6aa74b8787de5e860dca1ed5a06f7f88f0c2c0cdae5ba7914cd",
        "0x0d0e806aff90038bedba8b085cb1443bc6c2cb7fd016d72bee6293ac1c38558f",
        "0x613b0e57e234a9772d98f05ac6115f8c5ab3a1a65ded992664858d11f91d191d",
        "0xe7531d4e6c3cdeaef4d76dd1d8c1e39c15fe98d96601cb65bd37ae7be91f9e11",
        "0xd3eded331eb6c5b5dc182796c5ba24984cb6dc69e382b30d63d94a38e1c8a3ae",
        "0xd1127502fe3e73e5d59d6803145ce9e23220c2b74cc942e1e314004992a8dea4",
        "0x9ac8c3edcb8d1ebe47958163e652926b64377d637f62a1b5f6190dbcc28629dc",
        "0xd892898714f879ee2be6c3d75ea75452aac59b5014e2a7465b3c12d31db8dd0a",
        "0x887d32314f79ff436db55afb78569d31d064b3680625ee59430327c512a6db6e",
        "0x887d32314f79ff436db55afb78569d31d064b3680625ee59430327c512a6db6e",
        "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
        "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
        "0xc7ad823817921ff520f48c293b671eba4a85ebc4710d9cb4a863fb2377cc0fb8",
        "0xe8c0d0f31721f0ba018f3f86ff436567b2519de0a583b5f3dcd82778a60ed65f",
        "0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
        "0xd349830a9bbd67bb5f2481271a5e17b6d38c2d226c9551d3b0f082fb23b8cadf",
        "0x3551ae49921db3887314de4fe29f77d974e6e23831ba579e13e51b35af1fbdea",
        "0x6ffbf7e2ec2c7d46b9f517f5279f141d8f90c3d2346603e3adf3b3c378e58ad7",
        "0xd8392fad0f2cadb51b75cc166bcde4f02818a6f68564fed09a007d0bb3be354a",
    ]
    pool = [
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    ]
    reserve_name = [
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Dai Stablecoin",
        "USD Coin",
        "Tether USD",
        "Tether USD",
        "Dai Stablecoin",
        "USD Coin",
        "Uniswap",
        "Tether USD",
        "Curve DAO Token",
        "Maker",
        "Tether USD",
        "Wrapped BTC",
        "Wrapped BTC",
        "Aave Token",
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Wrapped BTC",
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "Tether USD",
        "USD Coin",
        "Tether USD",
        "Dai Stablecoin",
        "Tether USD",
        "Tether USD",
        "USD Coin",
        "Curve DAO Token",
        "Uniswap",
        "Maker",
        "Curve DAO Token",
    ]
    user_id = [
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x7369e5c97db67c30f64ae2924728c952848713b6",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
        "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
    ]

    return DataFrame(
        {
            "action": action,
            "amount": amount,
            "timestamp": timestamp,
            "txHash": txHash,
            "pool": pool,
            "reserve_name": reserve_name,
            "user_id": user_id,
        }
    )


@pytest.fixture
def liquidations_events():
    action = ["LiquidationCall", "LiquidationCall"]
    borrowAssetPriceUSD = [1.00012743, 1.00012743]
    collateralAmount = [0.5330296907207208, 0.34487589]
    collateralAssetPriceUSD = [1782.44, 0.0]
    id = [
        "18927469:42:0xfd11d768927ac040e9af6a68cb19c4a0a4bd2cc911ef62370bb8e0ffb4e511ba:96:96",
        "18926737:23:0x2a8059bca1bfbb21dac6cd958521ce33cfc7dd70a43e178e8eab64111f0c4de2:122:122",
    ]
    liquidator = [
        "0x681d0d7196a036661b354fa2a7e3b73c2adc43ec",
        "0x5c5c1776c16ef823e6582dd2747018ca5638e972",
    ]
    principalAmount = [1124.092325, 13615.95208]
    timestamp = [1704292631, 1704283799]
    txHash = [
        "0xfd11d768927ac040e9af6a68cb19c4a0a4bd2cc911ef62370bb8e0ffb4e511ba",
        "0x2a8059bca1bfbb21dac6cd958521ce33cfc7dd70a43e178e8eab64111f0c4de2",
    ]
    collateral_reserve_decimals = [18, 8]
    collateral_reserve_name = ["Wrapped Ether", "Wrapped BTC"]
    collateralReserve_underlyingAsset = [
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    ]
    pool = [
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    ]
    principal_reserve_decimals = [6, 6]
    principal_reserve_name = ["USD Coin", "USD Coin"]
    principalReserve_underlyingAsset = [
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    ]
    user_id = [
        "0xf94b51047c73ada249d3bac2f7e9c6c26b8d232c",
        "0x2d3178af3dfbb679716cc14e245be0a9e5945500",
    ]

    return DataFrame(
        {
            "action": action,
            "borrowAssetPriceUSD": borrowAssetPriceUSD,
            "collateralAmount": collateralAmount,
            "collateralAssetPriceUSD": collateralAssetPriceUSD,
            "id": id,
            "liquidator": liquidator,
            "principalAmount": principalAmount,
            "timestamp": timestamp,
            "txHash": txHash,
            "collateral_reserve_decimals": collateral_reserve_decimals,
            "collateral_reserve_name": collateral_reserve_name,
            "collateralReserve.underlyingAsset": collateralReserve_underlyingAsset,
            "pool": pool,
            "principal_reserve_decimals": principal_reserve_decimals,
            "principal_reserve_name": principal_reserve_name,
            "principalReserve.underlyingAsset": principalReserve_underlyingAsset,
            "user_id": user_id,
        }
    )


def test_clean_atokens_vtokens_balances(atokens):
    combined_abalances = clean_atokens_vtokens_balances(atokens)
    assert combined_abalances.columns.tolist() == [
        "id",
        "user_address",
        "timestamp",
        "pool",
        "reserve_decimals",
        "reserve_name",
        "usage_as_collateral_enabled",
        "user_current_atoken_balance",
    ]
    assert len(combined_abalances) == len(atokens)
    assert len(atokens.user_address.unique().tolist()) == len(
        combined_abalances[
            combined_abalances.user_current_atoken_balance.notna()
        ].drop_duplicates("user_address")
    )

    combined_abalances = combined_abalances.set_index(
        ["user_address", "timestamp", "reserve_name"]
    )

    assert (
        combined_abalances.loc[
            ("0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4", 1704744455, "Wrapped BTC"),
            "user_current_atoken_balance",
        ]
        == 0.00131934
    )
    assert (
        combined_abalances.loc[
            ("0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4", 1704744455, "Tether USD"),
            "user_current_atoken_balance",
        ]
        == 78113.96592
    )
    assert (
        combined_abalances.loc[
            ("0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4", 1704654371, "Wrapped BTC"),
            "user_current_atoken_balance",
        ]
        == 1.67214987
    )

    assert (
        combined_abalances.loc[
            ("0x7369e5c97db67c30f64ae2924728c952848713b6", 1704069563, "Aave Token"),
            "user_current_atoken_balance",
        ]
        == 76.235692
    )


def test_clean_events(events):
    clean_interaction_events = clean_events(events)
    assert clean_interaction_events.columns.tolist() == [
        "txHash",
        "user_address",
        "reserve_name",
        "timestamp",
        "pool",
        "action",
        "a_amount",
        "v_amount",
    ]
    assert len(clean_interaction_events) == 33
    assert clean_interaction_events[
        clean_interaction_events.action.isin(["Borrow", "Repay"])
    ].a_amount.unique().tolist() == [0]
    assert clean_interaction_events[
        clean_interaction_events.action.isin(["Supply", "RedeemUnderlying"])
    ].v_amount.unique().tolist() == [0]

    clean_interaction_events = clean_interaction_events.set_index(
        ["user_address", "txHash", "reserve_name"]
    )

    # Amount Values
    assert (
        np.round(
            clean_interaction_events.loc[
                (
                    "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                    "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
                    "Tether USD",
                ),
                "v_amount",
            ],
            3,
        )
        == 146721.205
    )
    assert (
        np.round(
            clean_interaction_events.loc[
                (
                    "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                    "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
                    "Tether USD",
                ),
                "a_amount",
            ],
            3,
        )
        == 0
    )
    assert (
        np.round(
            clean_interaction_events.loc[
                (
                    "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                    "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
                    "Dai Stablecoin",
                ),
                "v_amount",
            ],
            3,
        )
        == -146762.935
    )
    assert (
        np.round(
            clean_interaction_events.loc[
                (
                    "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                    "0x2590b86fc1ad95ac36197f24652812a4a5a91094f51310e6404488e48eca1ae8",
                    "Dai Stablecoin",
                ),
                "a_amount",
            ],
            3,
        )
        == 0
    )
    assert (
        np.round(
            clean_interaction_events.loc[
                (
                    "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                    "0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
                    "USD Coin",
                ),
                "v_amount",
            ],
            3,
        )
        == -155000.0
    )

    # Actions
    assert (
        clean_interaction_events.loc[
            (
                "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                "0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
                "USD Coin",
            ),
            "action",
        ]
        == "Repay"
    )
    assert (
        clean_interaction_events.loc[
            (
                "0x6642f3f6b05e5b98a32c5ad21db13bc607c53bb4",
                "0x79109534df8c257cbdfc9c93eaba2734f7d5fb10f5e458cec6d87e1f13da6d17",
                "Tether USD",
            ),
            "action",
        ]
        == "Supply"
    )


def test_clean_liquidation_events(liquidations_events):
    clean_liq_events = clean_liquidation_events(liquidations_events)
    assert clean_liq_events.columns.tolist() == [
        "user_address",
        "txHash",
        "reserve_name",
        "timestamp",
        "pool",
        "action",
    ]
    assert len(clean_liq_events) == 4 * len(liquidations_events)

    clean_liq_events = clean_liq_events.set_index(
        ["user_address", "reserve_name", "txHash"]
    )

    assert (
        clean_liq_events.loc[
            (
                "0xf94b51047c73ada249d3bac2f7e9c6c26b8d232c",
                "USD Coin",
                "0xfd11d768927ac040e9af6a68cb19c4a0a4bd2cc911ef62370bb8e0ffb4e511ba",
            ),
            "action",
        ]
        == "is_liquidated"
    )
    assert (
        clean_liq_events.loc[
            (
                "0x5c5c1776c16ef823e6582dd2747018ca5638e972",
                "Wrapped BTC",
                "0x2a8059bca1bfbb21dac6cd958521ce33cfc7dd70a43e178e8eab64111f0c4de2",
            ),
            "action",
        ]
        == "trigger_liquidation"
    )


def test_match_balances_with_events(balances, events, liquidations_events):
    clean_interaction_events = clean_events(events)
    clean_liquidations = clean_liquidation_events(liquidations_events)

    matched_balances_events = match_balances_with_events(
        balances, clean_interaction_events, clean_liquidations
    )

    assert matched_balances_events.columns.tolist() == [
        "id",
        "user_address",
        "timestamp",
        "pool",
        "reserve_decimals",
        "reserve_name",
        "usage_as_collateral_enabled",
        "user_current_vtoken_balance",
        "txHash",
        "action",
        "a_amount",
        "v_amount",
    ]

    assert len(matched_balances_events) == len(balances)

    matched_balances_events = matched_balances_events.set_index(
        ["user_address", "reserve_name", "txHash"]
    )
    assert (
        matched_balances_events.loc[
            (
                "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                "USD Coin",
                "0xe626ca8882664456b5e2540f6855a06374c747f361f10d7b4b07555f10a53b36",
            ),
            "v_amount",
        ]
        == 268000.0
    )
    assert (
        matched_balances_events.loc[
            (
                "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                "Uniswap",
                "0xdbb4f030cb8b98c253c4f0206cfd93da905e3fa84f14f0da1bc103c7b8814758",
            ),
            "action",
        ]
        == "Borrow"
    )
    assert (
        matched_balances_events.loc[
            (
                "0xb80997cbfe871a100c734c6d4527d0abc375a0f0",
                "USD Coin",
                "0xcd8a6e0c6a76142a79082eba387c8369bf7b5ece2f3435389f97126e482a2ffb",
            ),
            "action",
        ]
        == "Repay"
    )
