import hashlib

password = "456321"
hash_object = hashlib.sha1(password.encode())
sha1_hash = hash_object.hexdigest()
print("SHA1:", sha1_hash)