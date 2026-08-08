#!/bin/bash

SECRET=$(openssl rand -base64 32)

echo "Generated secret: ${SECRET}"
