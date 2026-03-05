import hashlib

personas = ["Fran", "Olaf", "Carla", "Hugo", "Eduardo", "Dexus", "Dvd", "Spam", "Itzantete", "Ainara", "Manolito", "Nico", "Lluc", "Ayna", "Morocha", "Paellita"]
base = ",".join(sorted(personas))
h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
print(f"Pool ID: {h}")
