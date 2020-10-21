// @flow

import createKeccakHash from 'keccak';
import secp256k1 from 'secp256k1';
import {randomBytes} from 'crypto';

import {Secp256k1Program, Secp256k1Instruction} from '../src/secp256k1-program';
import {mockRpcEnabled} from './__mocks__/node-fetch';
import {url} from './url';
import {
  Connection,
  Account,
  sendAndConfirmTransaction,
  LAMPORTS_PER_SOL,
  Transaction,
} from '../src';

const {privateKeyVerify, ecdsaSign, publicKeyCreate} = secp256k1;

if (!mockRpcEnabled) {
  jest.setTimeout(20000);
}

test('decode secp256k1 instruction', () => {
  let privateKey;
  do {
    privateKey = randomBytes(32);
  } while (!privateKeyVerify(privateKey));

  const instruction = Secp256k1Program.createInstructionWithPrivateKey({
    privateKey,
    message: Buffer.from('Test message'),
  });

  let {
    numSignatures,
    signatureOffset,
    signatureInstructionOffset,
    ethAddressOffset,
    ethAddressInstructionIndex,
    messageDataOffset,
    messageDataSize,
    messageInstructionIndex,
    signature,
    ethPublicKey,
    recoveryId,
    message,
  } = Secp256k1Instruction.decodeInstruction(instruction);

  expect(numSignatures).toEqual(1);
  expect(signatureOffset).toEqual(32);
  expect(signatureInstructionOffset).toEqual(0);
  expect(ethAddressOffset).toEqual(12);
  expect(ethAddressInstructionIndex).toEqual(0);
  expect(messageDataOffset).toEqual(97);
  expect(messageDataSize).toEqual(Buffer.from('Test message').length);
  expect(messageInstructionIndex).toEqual(0);
  expect(recoveryId > -1).toEqual(true);
  expect(signature.length).toEqual(64);
  expect(ethPublicKey.length).toEqual(20);
  expect(message.toString('utf8')).toEqual('Test message');
});

test('live create secp256k1 instruction with public key', async () => {
  if (mockRpcEnabled) {
    console.log('non-live test skipped');
    return;
  }

  const message = Buffer.from('This is a message');

  let privateKey;
  do {
    privateKey = randomBytes(32);
  } while (!privateKeyVerify(privateKey));

  const publicKey = publicKeyCreate(privateKey, false);
  const messageHash = createKeccakHash('keccak256').update(message).digest();
  const {signature, recid: recoveryId} = ecdsaSign(messageHash, privateKey);

  const instruction = Secp256k1Program.createInstructionWithPublicKey({
    publicKey,
    message,
    signature,
    recoveryId,
  });

  const transaction = new Transaction();
  transaction.add(instruction);

  const connection = new Connection(url, 'recent');
  const from = new Account();
  await connection.requestAirdrop(from.publicKey, 2 * LAMPORTS_PER_SOL);

  await sendAndConfirmTransaction(connection, transaction, [from], {
    commitment: 'single',
    skipPreflight: true,
  });
});

test('live create secp256k1 instruction with private key', async () => {
  if (mockRpcEnabled) {
    console.log('non-live test skipped');
    return;
  }

  let privateKey;
  do {
    privateKey = randomBytes(32);
  } while (!privateKeyVerify(privateKey));

  const instruction = Secp256k1Program.createInstructionWithPrivateKey({
    privateKey,
    message: Buffer.from('Test 123'),
  });

  const transaction = new Transaction();
  transaction.add(instruction);

  const connection = new Connection(url, 'recent');
  const from = new Account();
  await connection.requestAirdrop(from.publicKey, 2 * LAMPORTS_PER_SOL);

  await sendAndConfirmTransaction(connection, transaction, [from], {
    commitment: 'single',
    skipPreflight: true,
  });
});
