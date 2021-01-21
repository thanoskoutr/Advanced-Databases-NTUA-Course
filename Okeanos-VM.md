# Okeanos VMs info

## Master
```
username: user
password:
Private IPv4: 192.168.0.1
Public IPv4: 83.212.79.226
```
## Slave
```
username: user
password:
Private IPv4: 192.168.0.2
```
## Localnet
```
192.168.0.0/24
```


# Connect with SSH

## Connect to Master 
From you computer, type:
```
ssh user@83.212.79.226
```
## Connect to Slave 
From Master, type:
```
ssh user@192.168.0.2
```


# Set up SSH Keys

## Add SSH public key to Master (Linux)
In order to transfer you SSH public key from your computer to Master, type:
```
ssh-copy-id user@83.212.79.226
```
## Add SSH public key to Master (Windows)
SSH to Master and add your public key to `authorized_keys`.

You can do this, with this command:
```
echo "YOUR_SSH_PUBLIC_KEY" >> ~/.ssh/authorized_keys
```
or open the `authorized_keys` file:
```
vim ~/.ssh/authorized_keys
```
and manually paste your public key (`id_rsa.pub`) at the end of the file.


# Transfer Files

## Transfer Files with rsync

### Rsync a file from Local to Remote
Rsync a file in your current directory e.g. `test.txt` in the home directory of remote:
```
rsync -v test.txt user@83.212.79.226:.
```
### Rsync a file from Remote to Local
Rsync a file from remote e.g. `test2.txt` in your current directory on local:
```
rsync -v user@83.212.79.226:test2.txt .
```

### Rsync a directory from Local to Remote
Rsync a directory in your current directory e.g. `test` in the home directory of remote:
```
rsync -vr test user@83.212.79.226:.
```
### Rsync a directory from Remote to Local
Rsync a directory from remote e.g. `test2` in your current directory on local:
```
rsync -vr user@83.212.79.226:test2 .
```



## Transfer Files with scp

### Copy a file from Local to Remote
Copy a file in your current directory e.g. `test.txt` in the home directory of remote:
```
scp test.txt user@83.212.79.226:.
```
### Copy a file from Remote to Local
Copy a file from remote e.g. `test2.txt` in your current directory on local:
```
scp user@83.212.79.226:test2.txt .
```

### Copy a directory from Local to Remote
Copy a directory in your current directory e.g. `test` in the home directory of remote:
```
scp -r test user@83.212.79.226:.
```
### Copy a directory from Remote to Local
Copy a directory from remote e.g. `test2` in your current directory on local:
```
scp -r user@83.212.79.226:test2 .
```

