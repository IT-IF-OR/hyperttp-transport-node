export async function dynamicImport(specifier: string): Promise<unknown> {
  return import(specifier);
}
