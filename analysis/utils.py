from math import ceil
import conf as CFG

"""
This file is a slightly edited version of 
https://github.com/sr-gi/bitcoin_tools/blob/master/bitcoin_tools/analysis/status/utils.py

No copyright infringement is intended.

Some parts have been copied or edited in order to work with python 3 and make the code clearer, 
the parts that were useless for our analysis have been discarded.


Copyright (c) 2016, Sergi Delgado Segura
All rights reserved.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""

def roundup_rate(fee_rate, fee_step=CFG.FEE_STEP):

    """
    Rounds up a given fee rate to the nearest fee_step (FEE_STEP by default). If the rounded value it the value itself,
    adds fee_step, assuring that the returning rate is always bigger than the given one.

    :param fee_rate: Fee rate to be rounded up.
    :type fee_rate: float
    :param fee_step: Value at which fee_rate will be round up (FEE_STEP by default)
    :type fee_step: int
    :return: The rounded up fee_rate.
    :rtype: int
    """

    # If the value to be rounded is already multiple of the fee step, we just add another step. Otherwise the value
    # is rounded up.
    if (fee_rate % fee_step) == 0.0:
        rate = int(fee_rate + fee_step)
    else:
        rate = int(ceil(fee_rate / float(10))) * 10

    # # If the rounded up value is
    # if rate >= MAX_FEE_PER_BYTE:
    #     rate = 0

    return rate

def deobfuscate_value(obfuscation_key, value):
    """
    De-obfuscate a given value parsed from the chainstate.

    :param obfuscation_key: Key used to obfuscate the given value (extracted from the chainstate).
    :type obfuscation_key: str
    :param value: Obfuscated value.
    :type value: str
    :return: The de-obfuscated value.
    :rtype: str.
    """

    l_value = len(value)
    l_obf = len(obfuscation_key)

    # Get the extended obfuscation key by concatenating the obfuscation key with itself until it is as large as the
    # value to be de-obfuscated.
    if l_obf < l_value:
        extended_key = (obfuscation_key * ((l_value / l_obf) + 1))[:l_value]
    else:
        extended_key = obfuscation_key[:l_value]

    r = format(int(value, 16) ^ int(extended_key, 16), 'x')

    # In some cases, the obtained value could be 1 byte smaller than the original, since the leading 0 is dropped off
    # when the formatting.
    if len(r) == l_value-1:
        r = r.zfill(l_value)

    assert len(value) == len(r)

    return r

def check_multisig(script, std=True):
    """
    Checks whether a given script is a multisig one. By default, only standard multisig script are accepted.

    :param script: The script to be checked.
    :type script: str
    :param std: Whether the script is standard or not.
    :type std: bool
    :return: True if the script is multisig (under the std restrictions), False otherwise.
    :rtype: bool
    """

    if std:
        # Standard bare Pay-to-multisig only accepts up to 3-3.
        r = range(81, 83)
    else:
        # m-of-n combination is valid up to 20.
        r = range(84, 101)

    if int(script[:2], 16) in r and script[2:4] in ["21", "41"] and script[-2:] == "ae":
        return True
    else:
        return False

def b128_encode(n):
    """ Performs the MSB base-128 encoding of a given value. Used to store variable integers (varints) in the LevelDB.
    The code is a port from the Bitcoin Core C++ source. Notice that the code is not exactly the same since the original
    one reads directly from the LevelDB.

    The encoding is used to store Satoshi amounts into the Bitcoin LevelDB (chainstate). Before encoding, values are
    compressed using txout_compress.

    The encoding can also be used to encode block height values into the format use in the LevelDB, however, those are
    encoded not compressed.

    Explanation can be found in:
        https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/serialize.h#L307L329
    And code:
        https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/serialize.h#L343#L358

    The MSB of every byte (x)xxx xxxx encodes whether there is another byte following or not. Hence, all MSB are set to
    one except from the very last. Moreover, one is subtracted from all but the last digit in order to ensure a
    one-to-one encoding. Hence, in order decode a value, the MSB is changed from 1 to 0, and 1 is added to the resulting
    value. Then, the value is multiplied to the respective 128 power and added to the rest.

    Examples:

        - 255 = 807F (0x80 0x7F) --> (1)000 0000 0111 1111 --> 0000 0001 0111 1111 --> 1 * 128 + 127 = 255
        - 4294967296 (2^32) = 8EFEFEFF (0x8E 0xFE 0xFE 0xFF 0x00) --> (1)000 1110 (1)111 1110 (1)111 1110 (1)111 1111
            0000 0000 --> 0000 1111 0111 1111 0111 1111 1000 0000 0000 0000 --> 15 * 128^4 + 127*128^3 + 127*128^2 +
            128*128 + 0 = 2^32


    :param n: Value to be encoded.
    :type n: int
    :return: The base-128 encoded value
    :rtype: hex str
    """

    l = 0
    tmp = []
    data = ""

    while True:
        tmp.append(n & 0x7F)
        if l != 0:
            tmp[l] |= 0x80
        if n <= 0x7F:
            break
        n = (n >> 7) - 1
        l += 1

    tmp.reverse()
    for i in tmp:
        data += format(i, '02x')
    return data

def b128_decode(data):
    """ Performs the MSB base-128 decoding of a given value. Used to decode variable integers (varints) from the LevelDB.
    The code is a port from the Bitcoin Core C++ source. Notice that the code is not exactly the same since the original
    one reads directly from the LevelDB.

    The decoding is used to decode Satoshi amounts stored in the Bitcoin LevelDB (chainstate). After decoding, values
    are decompressed using txout_decompress.

    The decoding can be also used to decode block height values stored in the LevelDB. In his case, values are not
    compressed.

    Original code can be found in:
        https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/serialize.h#L360#L372

    Examples and further explanation can be found in b128_encode function.

    :param data: The base-128 encoded value to be decoded.
    :type data: hex str
    :return: The decoded value
    :rtype: int
    """

    n = 0
    i = 0
    while True:
        d = int(data[2 * i:2 * i + 2], 16)
        n = n << 7 | d & 0x7F
        if d & 0x80:
            n += 1
            i += 1
        else:
            return n

def parse_b128(utxo, offset=0):
    """ Parses a given serialized UTXO to extract a base-128 varint.

    :param utxo: Serialized UTXO from which the varint will be parsed.
    :type utxo: hex str
    :param offset: Offset where the beginning of the varint if located in the UTXO.
    :type offset: int
    :return: The extracted varint, and the offset of the byte located right after it.
    :rtype: hex str, int
    """

    data = utxo[offset:offset+2]
    offset += 2
    more_bytes = int(data, 16) & 0x80  # MSB b128 Varints have set the bit 128 for every byte but the last one,
    # indicating that there is an additional byte following the one being analyzed. If bit 128 of the byte being read is
    # not set, we are analyzing the last byte, otherwise, we should continue reading.
    while more_bytes:
        data += utxo[offset:offset+2]
        more_bytes = int(utxo[offset:offset+2], 16) & 0x80
        offset += 2

    return data, offset

def decode_utxo(coin, outpoint, version=0.15):
    """
    Decodes a LevelDB serialized UTXO for Bitcoin core v 0.15 onwards. The serialized format is defined in the Bitcoin
    Core source code as outpoint:coin.

    Outpoint structure is as follows: key | tx_hash | index.

    Where the key corresponds to b'C', or 43 in hex. The transaction hash in encoded in Little endian, and the index
    is a base128 varint. The corresponding Bitcoin Core source code can be found at:

    https://github.com/bitcoin/bitcoin/blob/ea729d55b4dbd17a53ced474a8457d4759cfb5a5/src/txdb.cpp#L40-L53

    On the other hand, a coin if formed by: code | value | out_type | script.

    Where code encodes the block height and whether the tx is coinbase or not, as 2*height + coinbase, the value is
    a txout_compressed base128 Varint, the out_type is also a base128 Varint, and the script is the remaining data.
    The corresponding Bitcoin Core soruce code can be found at:

    https://github.com/bitcoin/bitcoin/blob/6c4fecfaf7beefad0d1c3f8520bf50bb515a0716/src/coins.h#L58-L64

    :param coin: The coin to be decoded (extracted from the chainstate)
    :type coin: str
    :param outpoint: The outpoint to be decoded (extracted from the chainstate)
    :type outpoint: str
    :param version: Bitcoin Core version that created the chainstate LevelDB
    :return; The decoded UTXO.
    :rtype: dict
    """

    if 0.08 <= version < 0.15:
        return decode_utxo_v08_v014(coin)
    elif version < 0.08:
        raise Exception("The utxo decoder only works for version 0.08 onwards.")
    else:
        # First we will parse all the data encoded in the outpoint, that is, the transaction id and index of the utxo.
        # Check that the input data corresponds to a transaction.
        assert outpoint[:2] == '43'
        # Check the provided outpoint has at least the minimum length (1 byte of key code, 32 bytes tx id, 1 byte index)
        assert len(outpoint) >= 68
        # Get the transaction id (LE) by parsing the next 32 bytes of the outpoint.
        tx_id = outpoint[2:66]
        # Finally get the transaction index by decoding the remaining bytes as a b128 VARINT
        tx_index = b128_decode(outpoint[66:])

        # Once all the outpoint data has been parsed, we can proceed with the data encoded in the coin, that is, block
        # height, whether the transaction is coinbase or not, value, script type and script.
        # We start by decoding the first b128 VARINT of the provided data, that may contain 2*Height + coinbase
        code, offset = parse_b128(coin)
        code = b128_decode(code)
        height = code >> 1
        coinbase = code & 0x01

        # The next value in the sequence corresponds to the utxo value, the amount of Satoshi hold by the utxo. Data is
        # encoded as a B128 VARINT, and compressed using the equivalent to txout_compressor.
        data, offset = parse_b128(coin, offset)
        amount = txout_decompress(b128_decode(data))

        # Finally, we can obtain the data type by parsing the last B128 VARINT
        out_type, offset = parse_b128(coin, offset)
        out_type = b128_decode(out_type)

        if out_type in [0, 1]:
            data_size = 40  # 20 bytes
        elif out_type in [2, 3, 4, 5]:
            data_size = 66  # 33 bytes (1 byte for the type + 32 bytes of data)
            offset -= 2
        # Finally, if another value is found, it represents the length of the following data, which is uncompressed.
        else:
            data_size = (out_type - NSPECIALSCRIPTS) * 2  # If the data is not compacted, the out_type corresponds
            # to the data size adding the number os special scripts (nSpecialScripts).

        # And the remaining data corresponds to the script.
        script = coin[offset:]

        # Assert that the script hash the expected length
        assert len(script) == data_size

        # And to conclude, the output can be encoded. We will store it in a list for backward compatibility with the
        # previous decoder
        out = [{'amount': amount, 'out_type': out_type, 'data': script}]


    # Even though there is just one output, we will identify it as outputs for backward compatibility with the previous
    # decoder.
    return {'tx_id': tx_id, 'index': tx_index, 'coinbase': coinbase, 'outs': out, 'height': height}

    #return {'outs': out, 'height': height}

def get_min_input_size(out, height, count_p2sh=False):
    """
    Computes the minimum size an input created by a given output type (parsed from the chainstate) will have.
    The size is computed in two parts, a fixed size that is non type dependant, and a variable size which
    depends on the output type.

    :param out: Output to be analyzed.
    :type out: dict
    :param height: Block height where the utxo was created. Used to set P2PKH min_size.
    :type height: int
    :param count_p2sh: Whether P2SH should be taken into account.
    :type count_p2sh: bool
    :return: The minimum input size of the given output type.
    :rtype: int
    """

    out_type = out["out_type"]
    script = out["data"]

    # Fixed size
    prev_tx_id = 32
    prev_out_index = 4
    nSequence = 4

    fixed_size = prev_tx_id + prev_out_index + nSequence

    # Variable size (depending on scripSig):
    # Public key size can be either 33 or 65 bytes, depending on whether the key is compressed or uncompressed. We wil
    # make them fall in one of the categories depending on the block height in which the transaction was included.
    #
    # Signatures size is contained between 71-73 bytes depending on the size of the S and R components of the signature.
    # Since we are looking for the minimum size, we will consider all signatures to be 71-byte long in order to define
    # a lower bound.

    if out_type is 0:
        # P2PKH
        # Bitcoin core starts using compressed pk in version (0.6.0, 30/03/12, around block height 173480)
        if height < 173480:
            # uncompressed keys
            scriptSig = 138  # PUSH sig (1 byte) + sig (71 bytes) + PUSH pk (1 byte) + uncompressed pk (65 bytes)
        else:
            # compressed keys
            scriptSig = 106  # PUSH sig (1 byte) + sig (71 bytes) + PUSH pk (1 byte) + compressed pk (33 bytes)
        scriptSig_len = 1
    elif out_type is 1:
        # P2SH
        # P2SH inputs can have arbitrary length. Defining the length of the original script by just knowing the hash
        # is infeasible. Two approaches can be followed in this case. The first one consists on considering P2SH
        # by defining the minimum length a script of such type could have. The other approach will be ignoring such
        # scripts when performing the dust calculation.
        if count_p2sh:
            # If P2SH UTXOs are considered, the minimum script that can be created has only 1 byte (OP_1 for example)
            scriptSig = 1
            scriptSig_len = 1
        else:
            # Otherwise, we will define the length as 0 and skip such scripts for dust calculation.
            scriptSig = -fixed_size
            scriptSig_len = 0
    elif out_type in [2, 3, 4, 5]:
        # P2PK
        # P2PK requires a signature and a push OP_CODE to push the signature into the stack. The format of the public
        # key (compressed or uncompressed) does not affect the length of the signature.
        scriptSig = 72  # PUSH sig (1 byte) + sig (71 bytes)
        scriptSig_len = 1
    else:
        # P2MS
        if check_multisig(script):
            # Multisig can be 15-15 at most.
            req_sigs = int(script[:2], 16) - 80  # OP_1 is hex 81
            scriptSig = 1 + (req_sigs * 72)  # OP_0 (1 byte) + 72 bytes per sig (PUSH sig (1 byte) + sig (71 bytes))
            scriptSig_len = int(ceil(scriptSig / float(256)))
        else:
            # All other types (non-standard outs)
            scriptSig = -fixed_size - 1  # Those scripts are marked with length -1 and skipped in dust calculation.
            scriptSig_len = 0

    var_size = scriptSig_len + scriptSig

    return fixed_size + var_size



